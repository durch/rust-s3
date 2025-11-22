extern crate base64;
extern crate md5;

use bytes::Bytes;
use futures_util::TryStreamExt;
use maybe_async::maybe_async;
use std::borrow::Cow;
use std::collections::HashMap;

use super::request_trait::{Request, ResponseData, ResponseDataStream};
use crate::bucket::Bucket;
use crate::error::S3Error;
use crate::retry;

use tokio_stream::StreamExt;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClientOptions {
    pub request_timeout: Option<std::time::Duration>,
    pub proxy: Option<reqwest::Proxy>,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_certs: bool,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_hostnames: bool,
}

#[cfg(feature = "with-tokio")]
pub(crate) fn client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
    let client = reqwest::Client::builder();

    let client = if let Some(timeout) = options.request_timeout {
        client.timeout(timeout)
    } else {
        client
    };

    let client = if let Some(ref proxy) = options.proxy {
        client.proxy(proxy.clone())
    } else {
        client
    };

    cfg_if::cfg_if! {
        if #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))] {
            let client = client.danger_accept_invalid_certs(options.accept_invalid_certs);
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))] {
            let client = client.danger_accept_invalid_hostnames(options.accept_invalid_hostnames);
        }
    }

    Ok(client.build()?)
}
// Temporary structure for making a request
pub struct ReqwestRequest<'a> {
    request: http::Request<Cow<'a, [u8]>>,
    client: reqwest::Client,
    pub sync: bool,
}

impl ReqwestRequest<'_> {
    fn build(&self) -> Result<reqwest::Request, S3Error> {
        Ok(self.request.clone().map(|b| b.into_owned()).try_into()?)
    }
}

#[maybe_async]
impl<'a> Request for ReqwestRequest<'a> {
    type Response = reqwest::Response;
    type HeaderMap = reqwest::header::HeaderMap;

    async fn response(&self) -> Result<Self::Response, S3Error> {
        let response = self.client.execute(self.build()?).await?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let response = retry! {self.response().await }?;
        let status_code = response.status().as_u16();
        let mut headers = response.headers().clone();
        let response_headers = headers
            .clone()
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    v.to_str()
                        .unwrap_or("could-not-decode-header-value")
                        .to_string(),
                )
            })
            .collect::<HashMap<String, String>>();
        // When etag=true, we extract the ETag header and return it as the body.
        // This is used for PUT operations (regular puts, multipart chunks) where:
        // 1. S3 returns an empty or non-useful response body
        // 2. The ETag header contains the essential information we need
        // 3. The calling code expects to get the ETag via response_data.as_str()
        //
        // Note: This approach means we discard any actual response body when etag=true,
        // but for the operations that use this (PUTs), the body is typically empty
        // or contains redundant information already available in headers.
        //
        // TODO: Refactor this to properly return the response body and access ETag
        // from headers instead of replacing the body. This would be a breaking change.
        let body_vec = if etag {
            if let Some(etag) = headers.remove("ETag") {
                Bytes::from(etag.to_str()?.to_string())
            } else {
                Bytes::from("")
            }
        } else {
            response.bytes().await?
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        use tokio::io::AsyncWriteExt;
        let response = retry! {self.response().await}?;

        let status_code = response.status();
        let mut stream = response.bytes_stream();

        while let Some(item) = stream.next().await {
            writer.write_all(&item?).await?;
        }

        Ok(status_code.as_u16())
    }

    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error> {
        let response = retry! {self.response().await}?;
        let status_code = response.status();
        let stream = response.bytes_stream().map_err(S3Error::Reqwest);

        Ok(ResponseDataStream {
            bytes: Box::pin(stream),
            status_code: status_code.as_u16(),
        })
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let response = retry! {self.response().await}?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }
}

impl<'a> ReqwestRequest<'a> {
    pub fn new(request: http::Request<Cow<'a, [u8]>>, bucket: &Bucket) -> Result<Self, S3Error> {
        Ok(Self {
            request,
            client: bucket.http_client(),
            sync: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bucket;
    use crate::creds::Credentials;
    use http_body_util::BodyExt;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[tokio::test]
    async fn test_build() {
        let http_request = http::Request::builder()
            .uri("https://example.com/foo?bar=1")
            .method(http::Method::POST)
            .header("h1", "v1")
            .header("h2", "v2")
            .body(b"sneaky".into())
            .unwrap();
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials()).unwrap();

        let mut r = ReqwestRequest::new(http_request, &bucket)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(r.method(), http::Method::POST);
        assert_eq!(r.url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.headers().get("h1").unwrap(), "v1");
        assert_eq!(r.headers().get("h2").unwrap(), "v2");
        let body = r.body_mut().take().unwrap().collect().await;
        assert_eq!(body.unwrap().to_bytes().as_ref(), b"sneaky");
    }
}
