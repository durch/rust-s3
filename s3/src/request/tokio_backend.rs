extern crate base64;
extern crate md5;

use bytes::Bytes;
use futures::TryStreamExt;
use hyper::client::HttpConnector;
use hyper::{Body, Client};
use maybe_async::maybe_async;
use std::collections::HashMap;
use time::OffsetDateTime;

use super::request_trait::{Request, ResponseData, ResponseDataStream};
use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::error::S3Error;
use crate::utils::now_utc;

use tokio_stream::StreamExt;

#[cfg(feature = "tokio-rustls-tls")]
pub use hyper_rustls::HttpsConnector;
#[cfg(feature = "use-tokio-native-tls")]
pub use hyper_tls::HttpsConnector;

#[cfg(feature = "use-tokio-native-tls")]
pub fn client(
    request_timeout: Option<std::time::Duration>,
) -> Result<Client<HttpsConnector<HttpConnector>>, S3Error> {
    let mut tls_connector_builder = native_tls::TlsConnector::builder();

    if cfg!(feature = "no-verify-ssl") {
        tls_connector_builder.danger_accept_invalid_hostnames(true);
        tls_connector_builder.danger_accept_invalid_certs(true);
    }

    let tls_connector = tokio_native_tls::TlsConnector::from(tls_connector_builder.build()?);

    let mut http_connector = HttpConnector::new();
    http_connector.set_connect_timeout(request_timeout);
    http_connector.enforce_http(false);
    let https_connector = HttpsConnector::from((http_connector, tls_connector));

    Ok(Client::builder().build::<_, hyper::Body>(https_connector))
}

#[cfg(all(
    feature = "with-tokio",
    not(feature = "use-tokio-native-tls"),
    not(feature = "tokio-rustls-tls")
))]
pub fn client(
    request_timeout: Option<std::time::Duration>,
) -> Result<Client<HttpConnector>, S3Error> {
    let mut http_connector = HttpConnector::new();
    http_connector.set_connect_timeout(request_timeout);
    http_connector.enforce_http(false);

    Ok(Client::builder().build::<_, hyper::Body>(http_connector))
}

#[cfg(all(feature = "tokio-rustls-tls", feature = "no-verify-ssl"))]
pub struct NoCertificateVerification {}
#[cfg(all(feature = "tokio-rustls-tls", feature = "no-verify-ssl"))]
impl rustls::client::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

#[cfg(feature = "tokio-rustls-tls")]
pub fn client(
    request_timeout: Option<std::time::Duration>,
) -> Result<Client<HttpsConnector<HttpConnector>>, S3Error> {
    let mut roots = rustls::RootCertStore::empty();
    rustls_native_certs::load_native_certs()?
        .into_iter()
        .for_each(|cert| {
            roots.add(&rustls::Certificate(cert.0)).unwrap();
        });

    #[allow(unused_mut)]
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    #[cfg(feature = "no-verify-ssl")]
    {
        let mut dangerous_config = rustls::ClientConfig::dangerous(&mut config);
        dangerous_config
            .set_certificate_verifier(std::sync::Arc::new(NoCertificateVerification {}));
    }

    let mut http_connector = HttpConnector::new();
    http_connector.set_connect_timeout(request_timeout);
    http_connector.enforce_http(false);
    let https_connector = HttpsConnector::from((http_connector, config));

    Ok(Client::builder().build::<_, hyper::Body>(https_connector))
}

// Temporary structure for making a request
pub struct HyperRequest<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
}

#[maybe_async]
impl<'a> Request for HyperRequest<'a> {
    type Response = http::Response<Body>;
    type HeaderMap = http::header::HeaderMap;

    async fn response(&self) -> Result<http::Response<Body>, S3Error> {
        // Build headers
        let headers = match self.headers().await {
            Ok(headers) => headers,
            Err(e) => return Err(e),
        };

        let client = self.bucket.http_client();

        let method = match self.command.http_verb() {
            HttpMethod::Delete => http::Method::DELETE,
            HttpMethod::Get => http::Method::GET,
            HttpMethod::Post => http::Method::POST,
            HttpMethod::Put => http::Method::PUT,
            HttpMethod::Head => http::Method::HEAD,
        };

        let request = {
            let mut request = http::Request::builder()
                .method(method)
                .uri(self.url()?.as_str());

            for (header, value) in headers.iter() {
                request = request.header(header, value);
            }

            request.body(Body::from(self.request_body()?))?
        };
        let response = client.request(request).await?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text =
                String::from_utf8(hyper::body::to_bytes(response.into_body()).await?.into())?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let response = self.response().await?;
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
        let body_vec = if etag {
            if let Some(etag) = headers.remove("ETag") {
                Bytes::from(etag.to_str()?.to_string())
            } else {
                Bytes::from("")
            }
        } else {
            hyper::body::to_bytes(response.into_body()).await?
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        use tokio::io::AsyncWriteExt;
        let response = self.response().await?;

        let status_code = response.status();
        let mut stream = response.into_body().into_stream();

        while let Some(item) = stream.next().await {
            writer.write_all(&item?).await?;
        }

        Ok(status_code.as_u16())
    }

    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error> {
        let response = self.response().await?;
        let status_code = response.status();
        let stream = response.into_body().into_stream().map_err(S3Error::Hyper);

        Ok(ResponseDataStream {
            bytes: Box::pin(stream),
            status_code: status_code.as_u16(),
        })
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let response = self.response().await?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }

    fn datetime(&self) -> OffsetDateTime {
        self.datetime
    }

    fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }

    fn command(&self) -> Command {
        self.command.clone()
    }

    fn path(&self) -> String {
        self.path.to_string()
    }
}

impl<'a> HyperRequest<'a> {
    pub async fn new(
        bucket: &'a Bucket,
        path: &'a str,
        command: Command<'a>,
    ) -> Result<HyperRequest<'a>, S3Error> {
        bucket.credentials_refresh().await?;
        Ok(Self {
            bucket,
            path,
            command,
            datetime: now_utc(),
            sync: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request::tokio_backend::HyperRequest;
    use crate::request::Request;
    use awscreds::Credentials;
    use http::header::{HOST, RANGE};

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[tokio::test]
    async fn url_uses_https_by_default() {
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials()).unwrap();
        let path = "/my-first/path";
        let request = HyperRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_https_by_default_path_style() {
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-first/path";
        let request = HyperRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();

        assert_eq!(*host, "custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_scheme_from_custom_region_if_defined() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials()).unwrap();
        let path = "/my-second/path";
        let request = HyperRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_scheme_from_custom_region_if_defined_with_path_style() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-second/path";
        let request = HyperRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();
        assert_eq!(*host, "custom-region".to_string());
    }

    #[tokio::test]
    async fn test_get_object_range_header() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-second/path";

        let request = HyperRequest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: None,
            },
        )
        .await
        .unwrap();
        let headers = request.headers().await.unwrap();
        let range = headers.get(RANGE).unwrap();
        assert_eq!(range, "bytes=0-");

        let request = HyperRequest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: Some(1),
            },
        )
        .await
        .unwrap();
        let headers = request.headers().await.unwrap();
        let range = headers.get(RANGE).unwrap();
        assert_eq!(range, "bytes=0-1");
    }
}
