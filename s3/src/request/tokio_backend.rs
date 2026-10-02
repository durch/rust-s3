extern crate base64;
extern crate md5;

use bytes::Bytes;
use futures_util::TryStreamExt;
use maybe_async::maybe_async;
use std::collections::HashMap;
use std::str::FromStr as _;
use time::OffsetDateTime;

use super::request_trait::{Request, ResponseData, ResponseDataStream};
use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::error::S3Error;
use crate::retry;
use crate::utils::now_utc;

use tokio_stream::StreamExt;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClientOptions {
    pub proxy: Option<reqwest::Proxy>,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_certs: bool,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_hostnames: bool,
}

#[cfg(feature = "with-tokio")]
pub(crate) fn client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
    // Request timeouts are applied to individual requests so changing a
    // Bucket's timeout does not require rebuilding this client (or lose its
    // proxy/TLS configuration).
    let client = reqwest::Client::builder();

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
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
}

#[maybe_async]
impl<'a> Request for ReqwestRequest<'a> {
    type Response = reqwest::Response;
    type HeaderMap = reqwest::header::HeaderMap;

    async fn response(&self) -> Result<Self::Response, S3Error> {
        let headers = self
            .headers()
            .await?
            .iter()
            .map(|(k, v)| {
                (
                    reqwest::header::HeaderName::from_str(k.as_str()),
                    reqwest::header::HeaderValue::from_str(v.to_str().unwrap_or_default()),
                )
            })
            .filter(|(k, v)| k.is_ok() && v.is_ok())
            .map(|(k, v)| (k.unwrap(), v.unwrap()))
            .collect();

        let client = self.bucket.http_client();

        let method = match self.command.http_verb() {
            HttpMethod::Delete => reqwest::Method::DELETE,
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Head => reqwest::Method::HEAD,
        };

        let request = client
            .request(method, self.url()?.as_str())
            .headers(headers)
            .body(self.request_body()?);

        let request = if let Some(timeout) = self.bucket.request_timeout {
            request.timeout(timeout)
        } else {
            request
        };

        let request = request.build()?;

        // println!("Request: {:?}", request);

        let response = client.execute(request).await?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        Ok(response)
    }

    async fn response_status(&self) -> Result<u16, S3Error> {
        retry! {
            async {
                let headers = self
                    .headers()
                    .await?
                    .iter()
                    .map(|(k, v)| {
                        (
                            reqwest::header::HeaderName::from_str(k.as_str()),
                            reqwest::header::HeaderValue::from_str(v.to_str().unwrap_or_default()),
                        )
                    })
                    .filter(|(k, v)| k.is_ok() && v.is_ok())
                    .map(|(k, v)| (k.unwrap(), v.unwrap()))
                    .collect();

                let client = self.bucket.http_client();

                let method = match self.command.http_verb() {
                    HttpMethod::Delete => reqwest::Method::DELETE,
                    HttpMethod::Get => reqwest::Method::GET,
                    HttpMethod::Post => reqwest::Method::POST,
                    HttpMethod::Put => reqwest::Method::PUT,
                    HttpMethod::Head => reqwest::Method::HEAD,
                };

                let request = client
                    .request(method, self.url()?.as_str())
                    .headers(headers)
                    .body(self.request_body()?);

                let request = if let Some(timeout) = self.bucket.request_timeout {
                    request.timeout(timeout)
                } else {
                    request
                };

                let request = request.build()?;
                let response = client.execute(request).await?;
                let status = response.status().as_u16();

                if status == 404 {
                    return Ok(status);
                }

                if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                    let text = response.text().await?;
                    return Err(S3Error::HttpFailWithBody(status, text));
                }

                Ok(status)
            }.await
        }
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
}

impl<'a> ReqwestRequest<'a> {
    pub async fn new(
        bucket: &'a Bucket,
        path: &'a str,
        command: Command<'a>,
    ) -> Result<ReqwestRequest<'a>, S3Error> {
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
    use crate::request::Request;
    use crate::request::tokio_backend::ReqwestRequest;
    use awscreds::Credentials;
    use http::header::{HOST, RANGE};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    fn timeout_test_bucket(endpoint: String, timeout: Option<Duration>) -> Bucket {
        let mut bucket = *Bucket::new(
            "timeout-test",
            crate::region::Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            fake_credentials(),
        )
        .unwrap()
        .with_path_style();
        bucket.set_request_timeout(timeout);
        bucket
    }

    fn delayed_http_response(
        response: &'static [u8],
        delay: Duration,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        mock_http_response(response, None, delay)
    }

    fn stalled_http_body(
        initial_response: &'static [u8],
        trailing_body: &'static [u8],
        delay: Duration,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        mock_http_response(initial_response, Some(trailing_body), delay)
    }

    fn mock_http_response(
        initial_response: &'static [u8],
        trailing_body: Option<&'static [u8]>,
        delay: Duration,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let accept_deadline = std::time::Instant::now() + Duration::from_secs(8);
            let (mut stream, _) = loop {
                if !matches!(stop_rx.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                    return;
                }
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= accept_deadline {
                            return;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return,
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).unwrap_or(0) == 0 {
                    return;
                }
                request.push(byte[0]);
                assert!(
                    request.len() < 16 * 1024,
                    "request header exceeded fixture bound"
                );
            }
            if trailing_body.is_none()
                && !matches!(
                    stop_rx.recv_timeout(delay),
                    Err(mpsc::RecvTimeoutError::Timeout)
                )
            {
                return;
            }
            if stream.write_all(initial_response).is_err() {
                return;
            }
            if let Some(trailing_body) = trailing_body {
                if !matches!(
                    stop_rx.recv_timeout(delay),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    return;
                }
                let _ = stream.write_all(trailing_body);
            }
            let _ = stream.flush();
        });
        (endpoint, stop_tx, server)
    }

    fn timeout_then_success_status_server() -> (String, mpsc::Sender<()>, thread::JoinHandle<usize>)
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut attempts = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(8);
            let Some(mut first) = accept_mock_request(&listener, &stop_rx, deadline) else {
                return attempts;
            };
            attempts += 1;
            if !read_mock_request(&mut first) {
                return attempts;
            }
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let first_response_deadline = std::time::Instant::now() + Duration::from_secs(3);
            loop {
                if !matches!(stop_rx.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                    return attempts;
                }
                match listener.accept() {
                    Ok((mut second, _)) => {
                        let _ = second.set_nonblocking(false);
                        let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
                        let _ = second.set_write_timeout(Some(Duration::from_secs(2)));
                        attempts += 1;
                        if read_mock_request(&mut second) {
                            let _ = second.write_all(response);
                            let _ = second.flush();
                        }
                        return attempts;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= first_response_deadline {
                            let _ = first.write_all(response);
                            let _ = first.flush();
                            return attempts;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return attempts,
                }
            }
        });
        (endpoint, stop_tx, server)
    }

    fn accept_mock_request(
        listener: &TcpListener,
        stop: &mpsc::Receiver<()>,
        deadline: std::time::Instant,
    ) -> Option<std::net::TcpStream> {
        loop {
            if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty))
                || std::time::Instant::now() >= deadline
            {
                return None;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).ok()?;
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                    return Some(stream);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return None,
            }
        }
    }

    fn read_mock_request(stream: &mut std::net::TcpStream) -> bool {
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte) {
                Ok(0) | Err(_) => return false,
                Ok(_) => request.push(byte[0]),
            }
            if request.len() >= 16 * 1024 {
                return false;
            }
        }
        true
    }

    #[tokio::test]
    async fn request_timeout_covers_delayed_response_headers() {
        let (endpoint, stop, server) = delayed_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), request.response()).await;
        let _ = stop.send(());
        server.join().unwrap();
        let result = result.expect("request should finish within the fixture bound");
        assert!(
            matches!(&result, Err(crate::error::S3Error::Reqwest(error)) if error.is_timeout()),
            "delayed headers should exceed the request timeout"
        );
    }

    #[tokio::test]
    async fn request_timeout_covers_body_stream_and_writer_consumption() {
        let (endpoint, stop, server) = stalled_http_body(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
            b"def",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let mut body_stream = bucket.get_object_stream("/object").await.unwrap();
        let mut prefix = Vec::new();
        while prefix.len() < 3 {
            let chunk = tokio::time::timeout(Duration::from_secs(1), body_stream.bytes.next())
                .await
                .expect("first body bytes should arrive promptly")
                .expect("response body should contain its initial bytes")
                .unwrap();
            prefix.extend_from_slice(&chunk);
        }
        assert_eq!(&prefix[..3], b"abc");
        let next = tokio::time::timeout(Duration::from_secs(2), body_stream.bytes.next())
            .await
            .expect("body timeout should be bounded");
        assert!(matches!(
            next.expect("body should report its timeout"),
            Err(crate::error::S3Error::Reqwest(ref error)) if error.is_timeout()
        ));
        let _ = stop.send(());
        server.join().unwrap();

        let (endpoint, stop, server) = stalled_http_body(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
            b"def",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let mut writer = Vec::new();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            bucket.get_object_to_writer("/object", &mut writer),
        )
        .await;
        let _ = stop.send(());
        server.join().unwrap();
        let result = result.expect("writer timeout should be bounded");
        assert!(matches!(
            result,
            Err(crate::error::S3Error::Reqwest(ref error)) if error.is_timeout()
        ));
        assert_eq!(writer, b"abc", "the complete prefix should be written once");
    }

    #[tokio::test]
    async fn request_timeout_can_be_removed_after_bucket_creation() {
        let (endpoint, stop, server) = delayed_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            Duration::from_millis(1500),
        );
        let mut bucket = *timeout_test_bucket(endpoint, None)
            .with_request_timeout(Duration::from_millis(500))
            .unwrap();
        bucket.set_request_timeout(None);
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), request.response()).await;
        let _ = stop.send(());
        server.join().unwrap();
        let response = response
            .expect("request should finish within the fixture bound")
            .expect("None should remove the prior per-request timeout");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn response_status_applies_timeout_and_retries_the_request() {
        let (endpoint, stop, server) = timeout_then_success_status_server();
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let status = tokio::time::timeout(Duration::from_secs(5), request.response_status()).await;
        let _ = stop.send(());
        let attempts = server.join().unwrap();
        let status = status
            .expect("status retry should remain bounded")
            .expect("the second response should succeed");
        assert_eq!(status, 200);
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn url_uses_https_by_default() {
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials()).unwrap();
        let path = "/my-first/path";
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
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
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
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
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
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
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
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

        let request = ReqwestRequest::new(
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
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));
        assert_eq!(headers.get("Accept").unwrap(), "application/octet-stream");
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("range"));
        assert!(!signed_headers.contains("content-length"));
        assert!(!signed_headers.contains("content-type"));

        let request = ReqwestRequest::new(
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
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("range"));
        assert!(!signed_headers.contains("content-length"));
        assert!(!signed_headers.contains("content-type"));

        let get = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();
        let headers = get.headers().await.unwrap();
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));

        let put = ReqwestRequest::new(
            &bucket,
            path,
            Command::PutObject {
                content: b"abc",
                content_type: "application/test",
                custom_headers: None,
                multipart: None,
            },
        )
        .await
        .unwrap();
        let headers = put.headers().await.unwrap();
        assert_eq!(headers.get("Content-Length").unwrap(), "3");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/test");
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("content-length"));
        assert!(signed_headers.contains("content-type"));
    }
}
