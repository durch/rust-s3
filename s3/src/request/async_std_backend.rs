use async_std::io::ReadExt;
use async_std::io::Write as AsyncWrite;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

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

static SURF_CLIENT: OnceLock<Result<surf::Client, String>> = OnceLock::new();

fn surf_client() -> Result<&'static surf::Client, S3Error> {
    SURF_CLIENT
        .get_or_init(|| {
            let config = surf::Config::new().set_timeout(None);
            // Surf's H1 client does not honor response Connection: close when
            // recycling a fully-read connection. The next request can then
            // fail on a connection the server told us to close. Hyper handles
            // Connection: close correctly and keeps its pool enabled.
            #[cfg(any(feature = "async-std-native-tls", feature = "async-std-rustls-tls"))]
            let config = config.set_http_keep_alive(false);
            let client: Result<surf::Client, _> = config.try_into();
            client.map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| S3Error::Surf(error.clone()))
}

#[derive(Clone, Copy)]
struct RequestDeadline {
    expires_at: Instant,
}

impl RequestDeadline {
    fn new(timeout: Duration) -> Option<Self> {
        Instant::now()
            .checked_add(timeout)
            .map(|expires_at| Self { expires_at })
    }

    fn remaining(self) -> Option<Duration> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
    }
}

struct DeadlineReader {
    body: surf::http::Body,
    deadline: RequestDeadline,
    timer: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync>>>,
    ended: bool,
}

impl DeadlineReader {
    fn new(body: surf::http::Body, deadline: RequestDeadline) -> Self {
        Self {
            body,
            deadline,
            timer: None,
            ended: false,
        }
    }
}

impl async_std::io::Read for DeadlineReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.as_mut().get_mut();
        if this.ended {
            return Poll::Ready(Ok(0));
        }
        let Some(remaining) = this.deadline.remaining() else {
            return Poll::Ready(Err(request_timeout_error()));
        };

        if this.timer.is_none() {
            this.timer = Some(Box::pin(async_std::task::sleep(remaining)));
        }
        if this
            .timer
            .as_mut()
            .expect("deadline timer was initialized")
            .as_mut()
            .poll(context)
            .is_ready()
        {
            return Poll::Ready(Err(request_timeout_error()));
        }

        match Pin::new(&mut this.body).poll_read(context, buffer) {
            Poll::Ready(Ok(0)) => {
                this.ended = true;
                Poll::Ready(Ok(0))
            }
            result => result,
        }
    }
}

fn request_timeout_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "S3 request timed out")
}

fn timeout_attempt_error() -> crate::request::RequestAttemptError {
    crate::request::RequestAttemptError::surf_send_error(surf::Error::new(
        surf::http::StatusCode::GatewayTimeout,
        request_timeout_error(),
    ))
}

async fn send_with_deadline(
    request: surf::RequestBuilder,
    timeout: Option<Duration>,
) -> Result<surf::Response, crate::request::RequestAttemptError> {
    // Start immediately before sending so request building and signing do not
    // consume the network deadline. Very large durations that cannot be
    // represented by `Instant` are effectively unbounded.
    let deadline = timeout.and_then(RequestDeadline::new);
    let send = surf_client()
        .map_err(crate::request::RequestAttemptError::from)?
        .send(request.build());
    let response = match deadline {
        Some(deadline) => {
            let remaining = deadline.remaining().ok_or_else(timeout_attempt_error)?;
            async_std::future::timeout(remaining, send)
                .await
                .map_err(|_| timeout_attempt_error())?
                .map_err(crate::request::RequestAttemptError::surf_send_error)?
        }
        None => send
            .await
            .map_err(crate::request::RequestAttemptError::surf_send_error)?,
    };

    Ok(apply_body_deadline(response, deadline))
}

fn apply_body_deadline(
    mut response: surf::Response,
    deadline: Option<RequestDeadline>,
) -> surf::Response {
    if let Some(deadline) = deadline {
        let length = response.len();
        let body = response.take_body();
        let mime = body.mime().clone();
        let reader = DeadlineReader::new(body, deadline);
        let mut wrapped =
            surf::http::Body::from_reader(async_std::io::BufReader::new(reader), length);
        wrapped.set_mime(mime);
        response.set_body(wrapped);
    }
    response
}

async fn surf_response_attempt(
    request: &SurfRequest<'_>,
) -> Result<surf::Response, crate::request::RequestAttemptError> {
    let headers = request.headers().await?;
    let builder = match request.command.http_verb() {
        HttpMethod::Get => surf_client()?.request(Method::Get, request.url()?),
        HttpMethod::Delete => surf_client()?.request(Method::Delete, request.url()?),
        HttpMethod::Put => surf_client()?.request(Method::Put, request.url()?),
        HttpMethod::Post => surf_client()?.request(Method::Post, request.url()?),
        HttpMethod::Head => surf_client()?.request(Method::Head, request.url()?),
    };
    let mut request_builder = builder.body(request.request_body()?);
    for (name, value) in headers.iter() {
        request_builder = request_builder.header(
            HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                .expect("Could not parse header name"),
            HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                .expect("Could not parse header value"),
        );
    }
    let mut response = send_with_deadline(request_builder, request.bucket.request_timeout).await?;
    if cfg!(feature = "fail-on-err") && !response.status().is_success() {
        let status = u16::from(response.status());
        let body = response.body_string().await.map_err(|error| {
            crate::request::RequestAttemptError::do_not_retry(S3Error::Surf(error.to_string()))
        })?;
        return Err(S3Error::HttpFailWithBody(status, body).into());
    }
    Ok(response)
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
        surf_response_attempt(self)
            .await
            .map_err(|error| error.error)
    }

    async fn response_status(&self) -> Result<u16, S3Error> {
        crate::request::retry_request(&self.command, || async {
            let headers = self.headers().await?;

            let request = match self.command.http_verb() {
                HttpMethod::Get => surf_client()?.request(Method::Get, self.url()?),
                HttpMethod::Delete => surf_client()?.request(Method::Delete, self.url()?),
                HttpMethod::Put => surf_client()?.request(Method::Put, self.url()?),
                HttpMethod::Post => surf_client()?.request(Method::Post, self.url()?),
                HttpMethod::Head => surf_client()?.request(Method::Head, self.url()?),
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

            let mut response = send_with_deadline(request, self.bucket.request_timeout).await?;
            let status = u16::from(response.status());

            if status == 404 {
                Ok(status)
            } else if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                let body = response.body_string().await.map_err(|error| {
                    crate::request::RequestAttemptError::do_not_retry(S3Error::Surf(
                        error.to_string(),
                    ))
                })?;
                Err(S3Error::HttpFailWithBody(status, body).into())
            } else {
                Ok(status)
            }
        })
        .await
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let mut response =
            crate::request::retry_request(&self.command, || surf_response_attempt(self)).await?;
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
        let mut response =
            crate::request::retry_request(&self.command, || surf_response_attempt(self)).await?;

        let status_code = response.status();

        let mut stream = response.take_body();
        async_std::io::copy(&mut stream, writer).await?;

        Ok(status_code.into())
    }

    async fn response_header(&self) -> Result<(HeaderMap, u16), S3Error> {
        let mut header_map = HeaderMap::new();
        let response =
            crate::request::retry_request(&self.command, || surf_response_attempt(self)).await?;
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
        let mut response =
            crate::request::retry_request(&self.command, || surf_response_attempt(self)).await?;
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

    #[derive(Clone, Copy)]
    enum PauseAt {
        BeforeHeaders,
        AfterHeaders,
        AfterPrefix(usize),
    }

    struct PausedServer {
        endpoint: String,
        request_line: async_std::channel::Receiver<String>,
        release: async_std::channel::Sender<()>,
        task: async_std::task::JoinHandle<Result<(), String>>,
    }

    impl PausedServer {
        async fn release(&self) {
            let _ = self.release.send(()).await;
        }

        async fn finish(self) -> Result<()> {
            self.release().await;
            async_std::future::timeout(std::time::Duration::from_secs(6), self.task)
                .await
                .map_err(|_| anyhow::anyhow!("mock server did not stop"))?
                .map_err(anyhow::Error::msg)?;
            Ok(())
        }
    }

    async fn start_paused_server(status: u16, body: Vec<u8>, pause_at: PauseAt) -> PausedServer {
        use async_std::io::{ReadExt, WriteExt};
        use std::time::Instant;

        let listener = async_std::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (request_tx, request_line) = async_std::channel::bounded(1);
        let (release, release_rx) = async_std::channel::bounded(1);
        let task = async_std::task::spawn(async move {
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            let (mut connection, _) =
                async_std::future::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .map_err(|_| "accept timed out".to_owned())?
                    .map_err(|error| format!("accept: {error}"))?;

            let mut request = Vec::new();
            let mut byte = [0; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if request.len() >= 16 * 1024 || Instant::now() >= deadline {
                    return Err("request header cap or deadline exceeded".to_owned());
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let count = async_std::future::timeout(remaining, connection.read(&mut byte))
                    .await
                    .map_err(|_| "request header read timed out".to_owned())?
                    .map_err(|error| format!("read request: {error}"))?;
                if count == 0 {
                    return Err("client closed before request headers completed".to_owned());
                }
                request.push(byte[0]);
            }
            let request_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned();
            request_tx
                .send(request_line)
                .await
                .map_err(|error| format!("notify request received: {error}"))?;

            let response_header = format!(
                "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let prefix_len = match pause_at {
                PauseAt::AfterPrefix(len) => len.min(body.len()),
                _ => 0,
            };

            if matches!(pause_at, PauseAt::BeforeHeaders) {
                async_std::future::timeout(std::time::Duration::from_secs(4), release_rx.recv())
                    .await
                    .map_err(|_| "release before headers timed out".to_owned())?
                    .map_err(|error| format!("release before headers: {error}"))?;
            }

            let initial_body = if matches!(pause_at, PauseAt::AfterPrefix(_)) {
                &body[..prefix_len]
            } else if matches!(pause_at, PauseAt::AfterHeaders) {
                &body[..0]
            } else {
                &body[..]
            };
            let initial_write = async {
                connection.write_all(response_header.as_bytes()).await?;
                connection.write_all(initial_body).await
            };
            // The timeout tests intentionally let the client close the socket
            // before this delayed write; that disconnect is expected.
            let _ =
                async_std::future::timeout(std::time::Duration::from_secs(2), initial_write).await;
            if matches!(pause_at, PauseAt::AfterHeaders | PauseAt::AfterPrefix(_)) {
                async_std::future::timeout(std::time::Duration::from_secs(4), release_rx.recv())
                    .await
                    .map_err(|_| "release before body timed out".to_owned())?
                    .map_err(|error| format!("release before body: {error}"))?;
                let tail = &body[prefix_len..];
                let _ = async_std::future::timeout(
                    std::time::Duration::from_secs(2),
                    connection.write_all(tail),
                )
                .await;
            }
            Ok(())
        });

        PausedServer {
            endpoint,
            request_line,
            release,
            task,
        }
    }

    fn timeout_bucket(endpoint: String, timeout: Option<std::time::Duration>) -> Box<Bucket> {
        let mut bucket = Bucket::new(
            "test-bucket",
            crate::region::Region::Custom {
                region: "test-region".to_owned(),
                endpoint,
            },
            fake_credentials(),
        )
        .unwrap()
        .with_path_style();
        bucket.set_request_timeout(timeout);
        bucket
    }

    #[cfg(all(
        feature = "with-async-std-hyper",
        not(any(feature = "async-std-native-tls", feature = "async-std-rustls-tls"))
    ))]
    async fn start_persistent_server() -> (
        String,
        async_std::task::JoinHandle<Result<Vec<String>, String>>,
    ) {
        use async_std::io::{ReadExt, WriteExt};
        use std::time::Instant;

        let listener = async_std::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = async_std::task::spawn(async move {
            let (mut connection, _) =
                async_std::future::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .map_err(|_| "persistent accept timed out".to_owned())?
                    .map_err(|error| format!("persistent accept: {error}"))?;
            let mut requests = Vec::new();
            for _ in 0..2 {
                let deadline = Instant::now() + std::time::Duration::from_secs(5);
                let mut request = Vec::new();
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    if request.len() >= 16 * 1024 || Instant::now() >= deadline {
                        return Err("persistent request exceeded bounds".to_owned());
                    }
                    let count = async_std::future::timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        connection.read(&mut byte),
                    )
                    .await
                    .map_err(|_| "persistent request read timed out".to_owned())?
                    .map_err(|error| format!("persistent request read: {error}"))?;
                    if count == 0 {
                        return Err("connection closed before second request".to_owned());
                    }
                    request.push(byte[0]);
                }
                requests.push(
                    String::from_utf8_lossy(&request)
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_owned(),
                );
                let response =
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
                async_std::future::timeout(
                    std::time::Duration::from_secs(2),
                    connection.write_all(response),
                )
                .await
                .map_err(|_| "persistent response write timed out".to_owned())?
                .map_err(|error| format!("persistent response write: {error}"))?;
            }
            Ok(requests)
        });
        (endpoint, task)
    }

    #[async_std::test]
    async fn request_timeout_deadline_covers_public_response_body_reads() {
        let server = start_paused_server(200, b"abcdef".to_vec(), PauseAt::AfterPrefix(3)).await;
        let bucket = timeout_bucket(
            server.endpoint.clone(),
            Some(std::time::Duration::from_millis(400)),
        );
        let request = SurfRequest::new(&bucket, "/object", Command::CopyObject { from: "/source" })
            .await
            .unwrap();

        let mut response =
            async_std::future::timeout(std::time::Duration::from_secs(3), request.response())
                .await
                .expect("headers should arrive before the outer bound")
                .expect("response should be returned before body consumption");
        let request_line = async_std::future::timeout(
            std::time::Duration::from_secs(2),
            server.request_line.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(request_line.starts_with("PUT "));

        async_std::task::sleep(std::time::Duration::from_millis(500)).await;
        let body_result = async_std::future::timeout(
            std::time::Duration::from_millis(200),
            response.body_bytes(),
        )
        .await
        .expect("body timeout should be shorter than the outer bound");
        assert!(
            body_result.is_err(),
            "raw Surf response body must keep its deadline"
        );
        server.finish().await.unwrap();
    }

    #[async_std::test]
    async fn request_timeout_deadline_covers_delayed_response_headers() {
        let server = start_paused_server(200, b"ok".to_vec(), PauseAt::BeforeHeaders).await;
        let mut bucket = timeout_bucket(server.endpoint.clone(), None);
        bucket.set_request_timeout(Some(std::time::Duration::from_millis(300)));
        let request = SurfRequest::new(&bucket, "/object", Command::CopyObject { from: "/source" })
            .await
            .unwrap();

        let (result, request_line) = async_std::future::timeout(
            std::time::Duration::from_secs(3),
            futures_util::future::join(request.response(), server.request_line.recv()),
        )
        .await
        .expect("header timeout should be shorter than outer bound");
        assert!(request_line.unwrap().starts_with("PUT "));
        assert!(matches!(result, Err(crate::error::S3Error::Surf(_))));
        server.finish().await.unwrap();
    }

    #[async_std::test]
    async fn request_timeout_deadline_preserves_exact_writer_prefix() {
        let server = start_paused_server(200, b"abcdef".to_vec(), PauseAt::AfterPrefix(3)).await;
        let bucket = timeout_bucket(
            server.endpoint.clone(),
            Some(std::time::Duration::from_millis(400)),
        );
        let request = SurfRequest::new(&bucket, "/object", Command::CopyObject { from: "/source" })
            .await
            .unwrap();

        let mut writer = Vec::new();
        let result = async_std::future::timeout(
            std::time::Duration::from_secs(3),
            request.response_data_to_writer(&mut writer),
        )
        .await
        .expect("writer copy should be bounded");
        assert!(matches!(
            result,
            Err(crate::error::S3Error::Io(error))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        let request_line = async_std::future::timeout(
            std::time::Duration::from_secs(2),
            server.request_line.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(request_line.starts_with("PUT "));
        assert_eq!(writer, b"abc");
        server.finish().await.unwrap();
    }

    #[async_std::test]
    async fn request_timeout_deadline_applies_to_lazy_stream_reads() {
        use futures_util::StreamExt;

        let server = start_paused_server(200, b"abcdef".to_vec(), PauseAt::AfterPrefix(3)).await;
        let bucket = timeout_bucket(
            server.endpoint.clone(),
            Some(std::time::Duration::from_millis(500)),
        );
        let request = SurfRequest::new(&bucket, "/object", Command::CopyObject { from: "/source" })
            .await
            .unwrap();

        let mut response = async_std::future::timeout(
            std::time::Duration::from_secs(3),
            request.response_data_to_stream(),
        )
        .await
        .expect("stream response should be available before consuming body")
        .unwrap();
        let mut prefix = Vec::new();
        while prefix.len() < 3 {
            let next = async_std::future::timeout(
                std::time::Duration::from_secs(2),
                response.bytes().next(),
            )
            .await
            .expect("initial prefix should arrive before the deadline")
            .expect("server should provide a prefix")
            .expect("prefix read should succeed");
            prefix.extend_from_slice(&next);
        }
        assert_eq!(&prefix[..3], b"abc");
        let next =
            async_std::future::timeout(std::time::Duration::from_secs(2), response.bytes().next())
                .await
                .expect("lazy stream read should observe the request deadline");
        assert!(matches!(
            next,
            Some(Err(crate::error::S3Error::Io(error)))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        server.finish().await.unwrap();
    }

    #[async_std::test]
    async fn request_timeout_deadline_covers_status_error_body() {
        let server =
            start_paused_server(403, b"access denied".to_vec(), PauseAt::AfterHeaders).await;
        let bucket = timeout_bucket(
            server.endpoint.clone(),
            Some(std::time::Duration::from_millis(400)),
        );
        let request = SurfRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();
        let result = async_std::future::timeout(
            std::time::Duration::from_secs(3),
            request.response_status(),
        )
        .await
        .expect("status path should be bounded");

        let request_line = async_std::future::timeout(
            std::time::Duration::from_secs(2),
            server.request_line.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(request_line.starts_with("GET "));
        if cfg!(feature = "fail-on-err") {
            assert!(matches!(result, Err(crate::error::S3Error::Surf(_))));
        } else {
            assert_eq!(result.unwrap(), 403);
        }
        server.finish().await.unwrap();
    }

    #[async_std::test]
    async fn request_timeout_none_removes_deadline() {
        use futures_util::future::{Either, select};

        let server = start_paused_server(200, b"ok".to_vec(), PauseAt::BeforeHeaders).await;
        let mut bucket = timeout_bucket(
            server.endpoint.clone(),
            Some(std::time::Duration::from_millis(100)),
        );
        bucket.set_request_timeout(None);
        let request = SurfRequest::new(&bucket, "/object", Command::CopyObject { from: "/source" })
            .await
            .unwrap();
        let client_future = Box::pin(request.response());
        let receive_future = Box::pin(server.request_line.recv());
        let mut client_future = match select(client_future, receive_future).await {
            Either::Left((result, _)) => {
                panic!("response unexpectedly completed early: {result:?}")
            }
            Either::Right((Ok(line), pending)) => {
                assert!(line.starts_with("PUT "));
                pending
            }
            Either::Right((Err(error), _)) => panic!("mock request notification failed: {error}"),
        };
        assert!(
            async_std::future::timeout(std::time::Duration::from_millis(250), &mut client_future,)
                .await
                .is_err()
        );
        server.release().await;
        let mut response =
            async_std::future::timeout(std::time::Duration::from_secs(2), client_future)
                .await
                .expect("None timeout should leave the response pending until released")
                .unwrap();
        assert_eq!(response.body_bytes().await.unwrap(), b"ok");
        server.finish().await.unwrap();
    }

    #[cfg(all(
        feature = "with-async-std-hyper",
        not(any(feature = "async-std-native-tls", feature = "async-std-rustls-tls"))
    ))]
    #[async_std::test]
    async fn private_hyper_client_reuses_connections() {
        let (endpoint, server) = start_persistent_server().await;
        let bucket = timeout_bucket(endpoint, Some(std::time::Duration::from_secs(3)));
        for _ in 0..2 {
            let request = SurfRequest::new(&bucket, "/object", Command::GetObject)
                .await
                .unwrap();
            let result = async_std::future::timeout(
                std::time::Duration::from_secs(3),
                request.response_data(false),
            )
            .await
            .expect("pooled request should complete")
            .unwrap();
            assert_eq!(result.as_slice(), b"ok");
        }
        let request_lines = async_std::future::timeout(std::time::Duration::from_secs(4), server)
            .await
            .expect("persistent server should finish")
            .unwrap();
        assert_eq!(request_lines.len(), 2);
        assert!(request_lines.iter().all(|line| line.starts_with("GET ")));
    }

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
