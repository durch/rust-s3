extern crate base64;
extern crate md5;

use maybe_async::maybe_async;
use std::borrow::Cow;

use super::request_trait::Request;
use crate::bucket::Bucket;
use crate::error::S3Error;

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
    type ResponseBody = reqwest::Body;

    async fn response(&self) -> Result<http::Response<reqwest::Body>, S3Error> {
        let response = self.client.execute(self.build()?).await?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        Ok(response.into())
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
