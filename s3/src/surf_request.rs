use async_std::io::{ReadExt, WriteExt};
use bytes::Bytes;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::bucket::Bucket;
use super::command::Command;
use crate::error::S3Error;
use time::OffsetDateTime;

use crate::command::HttpMethod;
use crate::request_trait::{Request, ResponseData};

use http::HeaderMap;
use maybe_async::maybe_async;
use surf::http::headers::{HeaderName, HeaderValue};
use surf::http::Method;

// Temporary structure for making a request
pub struct SurfRequest<'a> {
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
    type ResponseStream = GetObjectStream;

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
            HttpMethod::Get => surf::Request::builder(Method::Get, self.url()),
            HttpMethod::Delete => surf::Request::builder(Method::Delete, self.url()),
            HttpMethod::Put => surf::Request::builder(Method::Put, self.url()),
            HttpMethod::Post => surf::Request::builder(Method::Post, self.url()),
            HttpMethod::Head => surf::Request::builder(Method::Head, self.url()),
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

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let mut response = self.response().await?;
        let status_code = response.status();

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
        Ok(ResponseData::new(body_vec, status_code.into()))
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

    async fn response_data_to_stream(&self) -> Result<(Self::ResponseStream, u16), S3Error> {
        let response = self.response().await?;
        let length = response.len();
        let status_code = response.status();

        let stream = surf::http::Body::from_reader(response, None);

        Ok((GetObjectStream::new(length, stream), status_code.into()))
    }
}

impl<'a> SurfRequest<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> SurfRequest<'b> {
        SurfRequest {
            bucket,
            path,
            command,
            datetime: OffsetDateTime::now_utc(),
            sync: false,
        }
    }
}

pub struct GetObjectStream {
    length: Option<usize>,
    inner: Pin<Box<dyn AsyncRead + Send>>,
}

impl GetObjectStream {
    pub(crate) fn new<R: 'static>(length: Option<usize>, reader: R) -> Self
    where
        R: AsyncRead + Send,
    {
        Self {
            length,
            inner: Box::pin(reader),
        }
    }
}

impl Stream for GetObjectStream {
    type Item = Result<Bytes, S3Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buffer = [0; 4096];

        let read = match self.inner.as_mut().poll_read(cx, &mut buffer) {
            Poll::Ready(Ok(s)) => s,
            Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
            Poll::Pending => return Poll::Pending,
        };

        if read == 0 {
            return Poll::Ready(None);
        }

        let bytes = Bytes::copy_from_slice(&buffer[..read]);
        Poll::Ready(Some(Ok(bytes)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.length)
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request_trait::Request;
    use crate::surf_request::SurfRequest;
    use anyhow::Result;
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
        let request = SurfRequest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "https");

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
        let request = SurfRequest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "https");

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
        let request = SurfRequest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "http");

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
        let request = SurfRequest::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }
}
