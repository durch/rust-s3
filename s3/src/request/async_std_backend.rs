use async_std::io::ReadExt;
use async_std::io::Write as AsyncWrite;
use bytes::Bytes;
use std::collections::HashMap;

use crate::bucket::Bucket;
use crate::command::Command;
use crate::error::S3Error;
use crate::utils::now_utc;
use time::OffsetDateTime;

use crate::command::HttpMethod;
use crate::request::{Request, ResponseData, ResponseDataStream};

use http::HeaderMap;
use maybe_async::maybe_async;
use surf::http::Method;
use surf::http::headers::{HeaderName, HeaderValue};

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

    fn datetime(&self) -> OffsetDateTime {
        self.datetime
    }

    fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }

    fn command(&self) -> Command<'_> {
        self.command.clone()
    }

    fn path(&self) -> String {
        self.path.to_string()
    }

    async fn response(&self) -> Result<surf::Response, S3Error> {
        // Build headers
        let headers = self.headers().await?;

        let request = match self.command.http_verb() {
            HttpMethod::Get => surf::Request::builder(Method::Get, self.url()?),
            HttpMethod::Delete => surf::Request::builder(Method::Delete, self.url()?),
            HttpMethod::Put => surf::Request::builder(Method::Put, self.url()?),
            HttpMethod::Post => surf::Request::builder(Method::Post, self.url()?),
            HttpMethod::Head => surf::Request::builder(Method::Head, self.url()?),
        };

        let mut request = request.body(self.request_body()?);

        for (name, value) in headers.iter() {
            request = request.header(
                HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                    .expect("Could not parse heaeder name"),
                HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                    .expect("Could not parse header value"),
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

    async fn response_status(&self) -> Result<u16, S3Error> {
        crate::retry! {
            async {
                let headers = self.headers().await?;

                let request = match self.command.http_verb() {
                    HttpMethod::Get => surf::Request::builder(Method::Get, self.url()?),
                    HttpMethod::Delete => surf::Request::builder(Method::Delete, self.url()?),
                    HttpMethod::Put => surf::Request::builder(Method::Put, self.url()?),
                    HttpMethod::Post => surf::Request::builder(Method::Post, self.url()?),
                    HttpMethod::Head => surf::Request::builder(Method::Head, self.url()?),
                };

                let mut request = request.body(self.request_body()?);

                for (name, value) in headers.iter() {
                    request = request.header(
                        HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                            .expect("Could not parse heaeder name"),
                        HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                            .expect("Could not parse header value"),
                    );
                }

                let response = request
                    .send()
                    .await
                    .map_err(|e| S3Error::Surf(e.to_string()))?;
                let status = u16::from(response.status());

                if status == 404 {
                    Ok(status)
                } else if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                    Err(S3Error::HttpFail)
                } else {
                    Ok(status)
                }
            }.await
        }
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
        let mut response = crate::retry! {self.response().await}?;

        let status_code = response.status();

        let mut stream = response.take_body();
        async_std::io::copy(&mut stream, writer).await?;

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

        let body = body_stream(response.take_body());

        Ok(ResponseDataStream {
            bytes: body,
            status_code: status_code.into(),
        })
    }
}

// Keep the body adapter separate so failure and backpressure behavior can be tested
// without an external object storage service.
fn body_stream(body: surf::http::Body) -> crate::request::DataStream {
    const CHUNK_SIZE: usize = 64 * 1024;

    Box::pin(futures_util::stream::try_unfold(
        body,
        |mut reader| async move {
            let mut buffer = vec![0; CHUNK_SIZE];
            let bytes_read = reader.read(&mut buffer).await.map_err(S3Error::Io)?;
            if bytes_read == 0 {
                return Ok(None);
            }

            buffer.truncate(bytes_read);
            Ok(Some((Bytes::from(buffer), reader)))
        },
    ))
}

impl<'a> SurfRequest<'a> {
    pub async fn new<'b>(
        bucket: &'b Bucket,
        path: &'b str,
        command: Command<'b>,
    ) -> Result<SurfRequest<'b>, S3Error> {
        bucket.credentials_refresh().await?;
        Ok(SurfRequest {
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
    use crate::request::Request;
    use crate::request::async_std_backend::SurfRequest;
    use anyhow::Result;
    use awscreds::Credentials;

    struct FailingReader {
        state: u8,
        offset: usize,
        reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl async_std::io::Read for FailingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buffer: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if buffer.is_empty() {
                return std::task::Poll::Ready(Ok(0));
            }
            if self.state == 0 {
                let count = (3 - self.offset).min(buffer.len());
                buffer[..count].copy_from_slice(&b"abc"[self.offset..self.offset + count]);
                self.offset += count;
                if self.offset == 3 {
                    self.state = 1;
                }
                std::task::Poll::Ready(Ok(count))
            } else if self.state == 1 {
                self.state = 2;
                std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "truncated object",
                )))
            } else {
                std::task::Poll::Ready(Ok(0))
            }
        }
    }

    #[async_std::test]
    async fn body_stream_preserves_read_errors() {
        use futures_util::StreamExt;
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = surf::http::Body::from_reader(
            async_std::io::BufReader::new(FailingReader {
                state: 0,
                offset: 0,
                reads: reads.clone(),
            }),
            None,
        );
        let mut stream = super::body_stream(body);
        assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"abc");
        assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(
            matches!(stream.next().await, Some(Err(crate::error::S3Error::Io(e)))
            if e.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        assert!(stream.next().await.is_none());
    }

    #[async_std::test]
    async fn body_stream_bounds_chunks_and_preserves_bytes() {
        use futures_util::StreamExt;
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let body = surf::http::Body::from_bytes(data.clone());
        let mut stream = super::body_stream(body);
        let first = stream.next().await.unwrap().unwrap();
        assert!(first.len() <= 64 * 1024);
        let mut received = first.to_vec();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(!chunk.is_empty() && chunk.len() <= 64 * 1024);
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, data);
        assert!(
            super::body_stream(surf::http::Body::empty())
                .next()
                .await
                .is_none()
        );

        let body =
            surf::http::Body::from_reader(async_std::io::Cursor::new(b"abcdef".to_vec()), Some(3));
        let mut stream = super::body_stream(body);
        assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"abc");
        assert!(stream.next().await.is_none());
    }

    struct FailingWriter;

    impl async_std::io::Write for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "writer closed",
            )))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[async_std::test]
    async fn response_data_to_writer_copies_body_and_propagates_writer_errors() {
        use async_std::io::{ReadExt, WriteExt};

        let listener = async_std::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let body: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let expected = body.clone();
        let server = async_std::task::spawn(async move {
            for request_number in 0..2 {
                let (mut connection, _) = async_std::future::timeout(
                    std::time::Duration::from_secs(10),
                    listener.accept(),
                )
                .await
                .unwrap()
                .unwrap();
                let mut request = Vec::new();
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    if connection.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                }
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let header_result = connection.write_all(headers.as_bytes()).await;
                if request_number == 0 {
                    header_result.unwrap();
                    connection.write_all(&body).await.unwrap();
                } else if header_result.is_ok() {
                    let _ = connection.write_all(&body).await;
                }
            }
        });

        let bucket = Bucket::new(
            "test-bucket",
            crate::region::Region::Custom {
                region: "test-region".to_owned(),
                endpoint,
            },
            fake_credentials(),
        )
        .unwrap()
        .with_path_style();
        let request = SurfRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let mut received = Vec::new();
        async_std::future::timeout(
            std::time::Duration::from_secs(10),
            request.response_data_to_writer(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received, expected);

        assert!(matches!(
            async_std::future::timeout(
                std::time::Duration::from_secs(10),
                request.response_data_to_writer(&mut FailingWriter),
            )
            .await
            .unwrap(),
            Err(crate::error::S3Error::Io(error))
                if error.kind() == std::io::ErrorKind::BrokenPipe
        ));
        async_std::future::timeout(std::time::Duration::from_secs(10), server)
            .await
            .unwrap();
    }

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[async_std::test]
    async fn url_uses_https_by_default() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = SurfRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url()?.scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[async_std::test]
    async fn url_uses_https_by_default_path_style() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-first/path";
        let request = SurfRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[async_std::test]
    async fn url_uses_scheme_from_custom_region_if_defined() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = SurfRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[async_std::test]
    async fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-second/path";
        let request = SurfRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }

    #[async_std::test]
    async fn range_get_omits_empty_body_headers_from_signature() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-second/path";

        for (start, end, expected_range) in [(0, None, "bytes=0-"), (10, Some(20), "bytes=10-20")] {
            let request =
                SurfRequest::new(&bucket, path, Command::GetObjectRange { start, end }).await?;
            let headers = request.headers().await?;
            assert_eq!(headers.get("Range").unwrap(), expected_range);
            assert_eq!(headers.get("Accept").unwrap(), "application/octet-stream");
            assert!(!headers.contains_key("Content-Length"));
            assert!(!headers.contains_key("Content-Type"));
            let authorization = headers.get("Authorization").unwrap().to_str()?;
            let signed_headers = authorization
                .split("SignedHeaders=")
                .nth(1)
                .and_then(|value| value.split(',').next())
                .unwrap();
            assert!(signed_headers.contains("range"));
            assert!(!signed_headers.contains("content-length"));
            assert!(!signed_headers.contains("content-type"));
        }

        let get = SurfRequest::new(&bucket, path, Command::GetObject).await?;
        let headers = get.headers().await?;
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));

        let put = SurfRequest::new(
            &bucket,
            path,
            Command::PutObject {
                content: b"abc",
                content_type: "application/test",
                custom_headers: None,
                multipart: None,
            },
        )
        .await?;
        let headers = put.headers().await?;
        assert_eq!(headers.get("Content-Length").unwrap(), "3");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/test");
        let authorization = headers.get("Authorization").unwrap().to_str()?;
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("content-length"));
        assert!(signed_headers.contains("content-type"));

        Ok(())
    }
}
