use async_std::io::Write as AsyncWrite;
use async_std::io::{ReadExt, WriteExt};
use async_std::stream::StreamExt;
use bytes::Bytes;
use futures_util::FutureExt;
use std::borrow::Cow;
use std::collections::HashMap;

use crate::bucket::Bucket;
use crate::error::S3Error;

use crate::request::{Request, ResponseData, ResponseDataStream};

use http::HeaderMap;
use maybe_async::maybe_async;
use surf::http::Method;
use surf::http::headers::{HeaderName, HeaderValue};

// Temporary structure for making a request
pub struct SurfRequest<'a> {
    request: http::Request<Cow<'a, [u8]>>,
    pub sync: bool,
}

impl SurfRequest<'_> {
    fn build(&self) -> Result<surf::RequestBuilder, S3Error> {
        let url = format!("{}", self.request.uri()).parse()?;
        let mut request = match *self.request.method() {
            http::Method::GET => surf::Request::builder(Method::Get, url),
            http::Method::DELETE => surf::Request::builder(Method::Delete, url),
            http::Method::PUT => surf::Request::builder(Method::Put, url),
            http::Method::POST => surf::Request::builder(Method::Post, url),
            http::Method::HEAD => surf::Request::builder(Method::Head, url),
            ref m => surf::Request::builder(
                m.as_str()
                    .parse()
                    .map_err(|e: surf::Error| S3Error::Surf(e.to_string()))?,
                url,
            ),
        }
        .body(self.request.body().clone().into_owned());

        for (name, value) in self.request.headers().iter() {
            request = request.header(
                HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                    .expect("Could not parse heaeder name"),
                HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                    .expect("Could not parse header value"),
            );
        }

        Ok(request)
    }
}

#[maybe_async]
impl<'a> Request for SurfRequest<'a> {
    type Response = surf::Response;
    type HeaderMap = HeaderMap;

    async fn response(&self) -> Result<surf::Response, S3Error> {
        let response = self
            .build()?
            .send()
            .await
            .map_err(|e| S3Error::Surf(e.to_string()))?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            return Err(S3Error::HttpFail);
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let mut response = crate::retry! {self.response().await}?;
        let status_code = response.status();

        let response_headers = response
            .header_names()
            .zip(response.header_values())
            .map(|(k, v)| (k.to_string(), v.to_string()))
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
            if let Some(etag) = response.header("ETag") {
                Bytes::from(etag.as_str().to_string())
            } else {
                Bytes::from("")
            }
        } else {
            let body = match response.body_bytes().await {
                Ok(bytes) => Ok(Bytes::from(bytes)),
                Err(e) => Err(S3Error::Surf(e.to_string())),
            };
            body?
        };
        Ok(ResponseData::new(
            body_vec,
            status_code.into(),
            response_headers,
        ))
    }

    async fn response_data_to_writer<T: AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        let mut buffer = Vec::new();

        let response = crate::retry! {self.response().await}?;

        let status_code = response.status();

        let mut stream = surf::http::Body::from_reader(response, None);

        stream.read_to_end(&mut buffer).await?;

        writer.write_all(&buffer).await?;

        Ok(status_code.into())
    }

    async fn response_header(&self) -> Result<(HeaderMap, u16), S3Error> {
        let mut header_map = HeaderMap::new();
        let response = crate::retry! {self.response().await}?;
        let status_code = response.status();

        for (name, value) in response.iter() {
            header_map.insert(
                http::header::HeaderName::from_lowercase(
                    name.to_string().to_ascii_lowercase().as_ref(),
                )?,
                value.as_str().parse()?,
            );
        }
        Ok((header_map, status_code.into()))
    }

    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error> {
        let mut response = crate::retry! {self.response().await}?;
        let status_code = response.status();

        let body = response
            .take_body()
            .bytes()
            .filter_map(|n| n.ok())
            .fold(vec![], |mut b, n| {
                b.push(n);
                b
            })
            .then(|b| async move { Ok(Bytes::from(b)) })
            .into_stream();

        Ok(ResponseDataStream {
            bytes: Box::pin(body),
            status_code: status_code.into(),
        })
    }
}

impl<'a> SurfRequest<'a> {
    pub fn new(request: http::Request<Cow<'a, [u8]>>, _: &Bucket) -> Result<Self, S3Error> {
        Ok(Self {
            request,
            sync: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bucket;
    use crate::creds::Credentials;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[async_std::test]
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

        let mut r = SurfRequest::new(http_request, &bucket)
            .unwrap()
            .build()
            .unwrap()
            .build();

        assert_eq!(r.method(), Method::Post);
        assert_eq!(r.url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.header("h1").unwrap(), "v1");
        assert_eq!(r.header("h2").unwrap(), "v2");
        let body = r.take_body().into_bytes().await.unwrap();
        assert_eq!(body.as_slice(), b"sneaky");
    }
}
