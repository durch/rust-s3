use async_std::io::{ReadExt, WriteExt};
use futures_io::AsyncWrite;

use super::bucket::Bucket;
use super::command::Command;
use crate::error::S3Error;
use time::OffsetDateTime;

use crate::command::HttpMethod;
use crate::request_trait::Request;

use http::HeaderMap;
use maybe_async::maybe_async;
use surf::http::headers::{HeaderName, HeaderValue};
use surf::http::Method;
use surf::Client;

// Temporary structure for making a request
pub struct SurfRequest<'a> {
    pub client: &'a Client,
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
}

#[maybe_async]
impl<'a> Request for SurfRequest<'a> {
    type Response = surf::Response;
    type HeaderMap = HeaderMap;

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

    async fn response(&self) -> Result<surf::Response, S3Error> {
        // Build headers
        let headers = self.headers()?;

        let request = match self.command.http_verb() {
            HttpMethod::Get => self.client.request(Method::Get, self.url()),
            HttpMethod::Delete => self.client.request(Method::Delete, self.url()),
            HttpMethod::Put => self.client.request(Method::Put, self.url()),
            HttpMethod::Post => self.client.request(Method::Post, self.url()),
            HttpMethod::Head => self.client.request(Method::Head, self.url()),
        };

        let mut request = request.body(self.request_body());

        for (name, value) in headers.iter() {
            request = request.header(
                HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec()).unwrap(),
                HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec()).unwrap(),
            );
        }

        let response = request
            .send()
            .await
            .map_err(|e| S3Error::Surf(e.to_string()))?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            return Err(S3Error::HttpFail);
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16), S3Error> {
        let mut response = self.response().await?;
        let status_code = response.status();
        let body = response
            .body_bytes()
            .await
            .map_err(|e| S3Error::Surf(e.to_string()))?;
        let mut body_vec = Vec::new();
        body_vec.extend_from_slice(&body[..]);
        if etag {
            if let Some(etag) = response.header("ETag") {
                body_vec = etag.as_str().to_string().as_bytes().to_vec();
            }
        }
        Ok((body_vec, status_code.into()))
    }

    async fn response_data_to_writer<T: AsyncWrite + Send + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        let mut buffer = Vec::new();

        let response = self.response().await?;

        let status_code = response.status();

        let mut stream = surf::http::Body::from_reader(response, None);

        stream.read_to_end(&mut buffer).await?;

        writer.write_all(&buffer).await?;

        Ok(status_code.into())
    }

    async fn response_header(&self) -> Result<(HeaderMap, u16), S3Error> {
        let mut header_map = HeaderMap::new();
        let response = self.response().await?;
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
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request_trait::Request;
    use crate::surf_request::SurfRequest;
    use crate::error::S3Error;
    use awscreds::Credentials;
    use time::OffsetDateTime;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[test]
    fn url_uses_https_by_default() -> Result<(), S3Error> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let client = surf::Client::new();
        let request = SurfRequest {
            client: &client,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: false,
        };

        assert_eq!(request.url().scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_https_by_default_path_style() -> Result<(), S3Error> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-first/path";
        let client = surf::Client::new();
        let request = SurfRequest {
            client: &client,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: false,
        };

        assert_eq!(request.url().scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined() -> Result<(), S3Error> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let client = surf::Client::new();
        let request = SurfRequest {
            client: &client,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: false,
        };

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<(), S3Error> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-second/path";
        let client = surf::Client::new();
        let request = SurfRequest {
            client: &client,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: false,
        };

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }
}
