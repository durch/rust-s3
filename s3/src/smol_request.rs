extern crate base64;
extern crate md5;

use std::io::Write;

use chrono::{DateTime, Utc};
use http_types::headers::Headers;
use http_types::Request as Req;
use http_types::{Method, Response};
use maybe_async::maybe_async;

use std::net::{TcpStream, ToSocketAddrs};

use anyhow::{bail, Context as _, Error, Result as anyResult};
use smol::{io::AsyncReadExt, Async};

use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::request_trait::Request;
use crate::{Result, S3Error};

use smol::stream::StreamExt;

impl std::convert::From<http_types::Error> for S3Error {
    fn from(e: http_types::Error) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None,
        }
    }
}

impl std::convert::From<anyhow::Error> for S3Error {
    fn from(e: anyhow::Error) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None,
        }
    }
}

/// Sends a request and fetches the response.
async fn fetch(req: Req) -> anyResult<Response> {
    // Figure out the host and the port.
    let host = req.url().host().context("cannot parse host")?.to_string();
    let port = req
        .url()
        .port_or_known_default()
        .context("cannot guess port")?;

    // Connect to the host.
    let socket_addr = {
        let host = host.clone();
        smol::unblock(move || (host.as_str(), port).to_socket_addrs())
            .await?
            .next()
            .context("cannot resolve address")?
    };
    let stream = Async::<TcpStream>::connect(socket_addr).await?;

    // Send the request and wait for the response.
    let resp = match req.url().scheme() {
        "http" => async_h1::connect(stream, req).await.map_err(Error::msg)?,
        "https" => {
            // In case of HTTPS, establish a secure TLS connection first.
            let stream = async_native_tls::connect(&host, stream).await?;
            async_h1::connect(stream, req).await.map_err(Error::msg)?
        }
        scheme => bail!("unsupported scheme: {}", scheme),
    };
    Ok(resp)
}

// Temporary structure for making a request
pub struct Srequest<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: DateTime<Utc>,
    pub sync: bool,
}

#[maybe_async(?Send)]
impl<'a> Request for Srequest<'a> {
    type Response = Response;
    type HeaderMap = Headers;

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

        let method = match self.command.http_verb() {
            HttpMethod::Delete => Method::Delete,
            HttpMethod::Get => Method::Get,
            HttpMethod::Post => Method::Post,
            HttpMethod::Put => Method::Put,
            HttpMethod::Head => Method::Head,
        };

        let mut request = Req::new(method, self.url(false).as_str());
        request.set_body(self.request_body());

        for (k, v) in headers.iter() {
            request.insert_header(k.as_str(), v.as_str());
        }

        let mut response = fetch(request).await?;

        if cfg!(feature = "fail-on-err") && response.status() as u16 >= 400 {
            return Err(S3Error::from(
                format!(
                    "Request failed with code {}\n{}",
                    response.status() as u16,
                    response.body_string().await?
                )
                .as_str(),
            ));
        }

        Ok(response)
    }

    async fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16)> {
        let mut response = self.response().await?;
        let res = response.clone();
        let status_code = res.status();
        let etag_header = res.header("ETag");
        let body = response.take_body();

        let bytes = body.into_bytes().await?;

        let mut body_vec = Vec::new();
        body_vec.extend_from_slice(&bytes[..]);
        if etag {
            if let Some(etag) = etag_header {
                body_vec = etag.to_string().as_bytes().to_vec();
            }
        }
        Ok((body_vec, status_code as u16))
    }

    async fn response_data_to_writer<'b, T: Write>(&self, writer: &'b mut T) -> Result<u16> {
        let response = self.response().await?;

        let status_code = response.status();
        let mut stream = response.bytes();

        while let Some(item) = stream.next().await {
            writer.write_all(&vec![item?])?;
        }

        Ok(status_code as u16)
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16)> {
        let response = self.response().await?;
        let status_code = response.status();
        let headers = response.as_ref().clone();
        Ok((headers, status_code as u16))
    }
}

impl<'a> Srequest<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Srequest<'b> {
        Srequest {
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
    use crate::request::Srequest;
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
        let request = Srequest::new(&bucket, path, Command::GetObject);

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
        let request = Srequest::new(&bucket, path, Command::GetObject);

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
        let request = Srequest::new(&bucket, path, Command::GetObject);

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
        let request = Srequest::new(&bucket, path, Command::GetObject);

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

        let request = Srequest::new(
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

        let request = Srequest::new(
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
