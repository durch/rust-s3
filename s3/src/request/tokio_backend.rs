extern crate base64;
extern crate md5;

use maybe_async::maybe_async;
use std::borrow::Cow;
use std::time::Duration;

use super::request_trait::Request;
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

fn client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
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

impl ReqwestBackend {
    pub(crate) fn request<'a>(&self, request: http::Request<Cow<'a, [u8]>>) -> ReqwestRequest<'a> {
        ReqwestRequest {
            request,
            client: self.http_client.clone(),
            sync: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReqwestBackend {
    http_client: reqwest::Client,
    client_options: ClientOptions,
}

impl ReqwestBackend {
    pub fn with_request_timeout(&self, request_timeout: Option<Duration>) -> Result<Self, S3Error> {
        let client_options = ClientOptions {
            request_timeout,
            ..self.client_options.clone()
        };
        Ok(Self {
            http_client: client(&client_options)?,
            client_options,
        })
    }

    pub fn request_timeout(&self) -> Option<Duration> {
        self.client_options.request_timeout
    }

    /// Configures a bucket to accept invalid SSL certificates and hostnames.
    ///
    /// This method is available only when either the `tokio-native-tls` or `tokio-rustls-tls` feature is enabled.
    ///
    /// # Parameters
    ///
    /// - `accept_invalid_certs`: A boolean flag that determines whether the client should accept invalid SSL certificates.
    /// - `accept_invalid_hostnames`: A boolean flag that determines whether the client should accept invalid hostnames.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the newly configured `Bucket` instance if successful, or an `S3Error` if an error occurs during client configuration.
    ///
    /// # Errors
    ///
    /// This function returns an `S3Error` if the HTTP client configuration fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use s3::bucket::Bucket;
    /// # use s3::error::S3Error;
    /// # use s3::creds::Credentials;
    /// # use s3::Region;
    /// # use std::str::FromStr;
    ///
    /// # fn example() -> Result<(), S3Error> {
    /// let bucket = Bucket::new("my-bucket", Region::from_str("us-east-1")?, Credentials::default()?)?
    ///     .set_dangereous_config(true, true)?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub fn with_dangereous_config(
        &self,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Result<Self, S3Error> {
        let client_options = ClientOptions {
            accept_invalid_certs,
            accept_invalid_hostnames,
            ..self.client_options.clone()
        };
        Ok(Self {
            http_client: client(&client_options)?,
            client_options,
        })
    }

    pub fn with_proxy(&self, proxy: reqwest::Proxy) -> Result<Self, S3Error> {
        let client_options = ClientOptions {
            proxy: Some(proxy),
            ..self.client_options.clone()
        };
        Ok(Self {
            http_client: client(&client_options)?,
            client_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn test_build() {
        let http_request = http::Request::builder()
            .uri("https://example.com/foo?bar=1")
            .method(http::Method::POST)
            .header("h1", "v1")
            .header("h2", "v2")
            .body(b"sneaky".into())
            .unwrap();

        let mut r = ReqwestBackend::default()
            .request(http_request)
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
