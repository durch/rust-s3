extern crate base64;
extern crate md5;

use std::io::Write;

use chrono::{DateTime, Utc};
use maybe_async::maybe_async;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response};

use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::request_trait::Request;
use crate::{Result, S3Error};

use tokio::stream::StreamExt;

impl std::convert::From<reqwest::Error> for S3Error {
    fn from(e: reqwest::Error) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None,
        }
    }
}

impl std::convert::From<reqwest::header::InvalidHeaderName> for S3Error {
    fn from(e: reqwest::header::InvalidHeaderName) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None,
        }
    }
}

impl std::convert::From<reqwest::header::InvalidHeaderValue> for S3Error {
    fn from(e: reqwest::header::InvalidHeaderValue) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None,
        }
    }
}

// Temporary structure for making a request
pub struct Reqwest<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: DateTime<Utc>,
    pub sync: bool,
}

#[maybe_async(?Send)]
impl<'a> Request for Reqwest<'a> {
    type Response = reqwest::Response;
    type HeaderMap = reqwest::header::HeaderMap;

    fn command(&self) -> Command {
        self.command.clone()
    }

    fn path(&self) -> String {
        self.path.to_string()
    }

    fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }

    fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }

    async fn response(&self) -> Result<Response> {
        // Build headers
        let headers = match self.headers() {
            Ok(headers) => headers,
            Err(e) => return Err(e),
        };

        let client = if cfg!(feature = "no-verify-ssl") {
            let client = Client::builder();

            cfg_if::cfg_if! {
                if #[cfg(feature = "tokio-native-tls")]
                {
                    let client = client.danger_accept_invalid_hostnames(true);
                }

            }

            cfg_if::cfg_if! {
                if #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
                {
                    let client = client.danger_accept_invalid_certs(true);
                }

            }

            client.build().expect("Could not build dangerous client!")
        } else {
            Client::new()
        };

        let method = match self.command.http_verb() {
            HttpMethod::Delete => reqwest::Method::DELETE,
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Head => reqwest::Method::HEAD,
        };

        let mut header_map = HeaderMap::new();

        for (k, v) in headers.into_iter() {
            header_map.insert(
                HeaderName::from_bytes(k.as_bytes())?,
                HeaderValue::from_bytes(v.as_bytes())?,
            );
        }

        let request = client
            .request(method, self.url(false).as_str())
            // TODO convert this
            .headers(header_map)
            .body(self.request_body());

        let response = request.send().await?;

        if cfg!(feature = "fail-on-err") && response.status().as_u16() >= 400 {
            return Err(S3Error::from(
                format!(
                    "Request failed with code {}\n{}",
                    response.status().as_u16(),
                    response.text().await?
                )
                .as_str(),
            ));
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16)> {
        let response = self.response().await?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        let etag_header = headers.get("ETag");
        let body = response.bytes().await?;
        let mut body_vec = Vec::new();
        body_vec.extend_from_slice(&body[..]);
        if etag {
            if let Some(etag) = etag_header {
                body_vec = etag.to_str()?.as_bytes().to_vec();
            }
        }
        Ok((body_vec, status_code))
    }

    async fn response_data_to_writer<'b, T: Write>(&self, writer: &'b mut T) -> Result<u16> {
        let response = self.response().await?;

        let status_code = response.status();
        let mut stream = response.bytes_stream();

        while let Some(item) = stream.next().await {
            writer.write_all(&item?)?;
        }

        Ok(status_code.as_u16())
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16)> {
        let response = self.response().await?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }
}

impl<'a> Reqwest<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Reqwest<'b> {
        Reqwest {
            bucket,
            path,
            command,
            datetime: Utc::now(),
            sync: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request::Reqwest;
    use crate::request_trait::Request;
    use crate::Result;
    use awscreds::Credentials;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[test]
    fn url_uses_https_by_default() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = Reqwest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_https_by_default_path_style() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new_with_path_style("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = Reqwest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = Reqwest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new_with_path_style("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = Reqwest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }

    #[test]
    fn test_get_object_range_header() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new_with_path_style("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";

        let request = Reqwest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: None,
            },
        );
        let headers = request.headers().unwrap();
        let range = headers.get("Range").unwrap();
        assert_eq!(range, "bytes=0-");

        let request = Reqwest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: Some(1),
            },
        );
        let headers = request.headers().unwrap();
        let range = headers.get("Range").unwrap();
        assert_eq!(range, "bytes=0-1");

        Ok(())
    }
}
