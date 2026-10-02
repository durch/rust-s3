//! Wire-level regression tests for request retry behavior.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::bucket::Bucket;
use crate::command::{Command, Multipart};
use crate::error::S3Error;
use crate::region::Region;
use crate::request::Request;
use awscreds::Credentials;

#[cfg(feature = "sync")]
type BackendRequest<'a> = crate::request::blocking::AttoRequest<'a>;
#[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
type BackendRequest<'a> = crate::request::tokio_backend::ReqwestRequest<'a>;
#[cfg(all(
    not(feature = "sync"),
    not(feature = "with-tokio"),
    feature = "with-async-std"
))]
type BackendRequest<'a> = crate::request::async_std_backend::SurfRequest<'a>;

#[derive(Debug)]
struct CapturedRequest {
    line: String,
    body: Vec<u8>,
}

enum Reply {
    Response { status: u16, body: &'static [u8] },
    MalformedChunkedResponse { status: u16, body: &'static [u8] },
    Close,
}

struct MockServer {
    endpoint: String,
    stop: mpsc::Sender<()>,
    thread: thread::JoinHandle<Result<Vec<CapturedRequest>, String>>,
}

impl MockServer {
    fn finish(self) -> Result<Vec<CapturedRequest>, String> {
        let _ = self.stop.send(());
        self.thread
            .join()
            .map_err(|_| "mock server thread panicked".to_owned())?
    }
}

fn start_server(replies: Vec<Reply>) -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local mock server");
    listener
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = mpsc::channel();
    let server_thread = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(12);
        let mut requests = Vec::new();

        for reply in replies {
            let (mut stream, _) = accept_until(&listener, deadline, &stop_rx)?;
            stream
                .set_nonblocking(false)
                .map_err(|error| format!("set accepted socket blocking: {error}"))?;
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .map_err(|error| format!("set read timeout: {error}"))?;
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .map_err(|error| format!("set write timeout: {error}"))?;
            let request = read_request(&mut stream, deadline)?;
            requests.push(request);

            match reply {
                Reply::Close => drop(stream),
                Reply::Response { status, body } => {
                    let reason = match status {
                        200 => "OK",
                        403 => "Forbidden",
                        503 => "Service Unavailable",
                        _ => "Test Response",
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .and_then(|()| stream.write_all(body))
                        .map_err(|error| format!("write mock response: {error}"))?;
                }
                Reply::MalformedChunkedResponse { status, body } => {
                    let response = format!(
                        "HTTP/1.1 {status} Forbidden\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                    );
                    stream
                        .write_all(response.as_bytes())
                        .and_then(|()| stream.write_all(body))
                        .map_err(|error| format!("write malformed chunked response: {error}"))?;
                    // Invalid chunk-size framing must surface as a real body
                    // read error instead of a partially accepted error body.
                }
            }
        }

        // Keep the listener alive until the request call has returned. Any
        // extra connection is an observable extra attempt rather than a race
        // against a server that already exited.
        loop {
            if stop_rx.try_recv().is_ok() {
                return Ok(requests);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "mock server deadline after {} request(s)",
                    requests.len()
                ));
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_nonblocking(false)
                        .map_err(|error| format!("set extra accepted socket blocking: {error}"))?;
                    stream
                        .set_read_timeout(Some(Duration::from_millis(100)))
                        .map_err(|error| format!("set extra read timeout: {error}"))?;
                    stream
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .map_err(|error| format!("set extra write timeout: {error}"))?;
                    let request = read_request(&mut stream, deadline)?;
                    requests.push(request);
                    let response = b"HTTP/1.1 599 Unexpected Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = stream.write_all(response);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(format!("accept extra request: {error}")),
            }
        }
    });

    MockServer {
        endpoint,
        stop: stop_tx,
        thread: server_thread,
    }
}

fn accept_until(
    listener: &TcpListener,
    deadline: Instant,
    stop: &mpsc::Receiver<()>,
) -> Result<(TcpStream, std::net::SocketAddr), String> {
    loop {
        if stop.try_recv().is_ok() {
            return Err("mock server stopped before expected request".to_owned());
        }
        if Instant::now() >= deadline {
            return Err("mock server accept deadline exceeded".to_owned());
        }
        match listener.accept() {
            Ok(connection) => return Ok(connection),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(format!("accept request: {error}")),
        }
    }
}

fn read_request(stream: &mut TcpStream, deadline: Instant) -> Result<CapturedRequest, String> {
    let mut headers = Vec::new();
    let mut byte = [0u8; 1];
    while !headers.ends_with(b"\r\n\r\n") {
        if headers.len() >= 16 * 1024 || Instant::now() >= deadline {
            return Err("request headers exceeded cap or deadline".to_owned());
        }
        match stream.read(&mut byte) {
            Ok(0) => return Err("client closed before request headers completed".to_owned()),
            Ok(_) => headers.push(byte[0]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(format!("read request headers: {error}")),
        }
    }

    let header_text = String::from_utf8_lossy(&headers);
    let mut lines = header_text.lines();
    let line = lines.next().unwrap_or_default().to_owned();
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > 1024 * 1024 {
        return Err("request body exceeded test cap".to_owned());
    }
    let mut body = vec![0; content_length];
    let mut offset = 0;
    while offset < body.len() {
        if Instant::now() >= deadline {
            return Err("request body deadline exceeded".to_owned());
        }
        match stream.read(&mut body[offset..]) {
            Ok(0) => return Err("client closed before request body completed".to_owned()),
            Ok(count) => offset += count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(format!("read request body: {error}")),
        }
    }

    Ok(CapturedRequest { line, body })
}

#[maybe_async::maybe_async]
async fn test_bucket(endpoint: String) -> Box<Bucket> {
    let credentials = Credentials::new(
        Some("retry-test-key"),
        Some("retry-test-secret"),
        None,
        None,
        None,
    )
    .unwrap();
    let region = Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint,
    };
    Bucket::new("retry-test-bucket", region, credentials)
        .unwrap()
        .with_path_style()
}

#[maybe_async::maybe_async]
async fn make_request<'a>(
    bucket: &'a Bucket,
    command: Command<'a>,
) -> Result<BackendRequest<'a>, S3Error> {
    BackendRequest::new(bucket, "/object", command).await
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_get_retries_transient_status_only_when_errors_are_enabled() {
    let replies = if cfg!(feature = "fail-on-err") {
        vec![
            Reply::Response {
                status: 503,
                body: b"temporary",
            },
            Reply::Response {
                status: 200,
                body: b"ok",
            },
        ]
    } else {
        vec![Reply::Response {
            status: 503,
            body: b"raw service response",
        }]
    };
    let server = start_server(replies);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_data(false).await;
    let requests = server.finish().expect("mock server should finish");

    if cfg!(feature = "fail-on-err") {
        let response = result.expect("retry should return successful response");
        assert_eq!(response.status_code(), 200);
        assert_eq!(response.as_slice(), b"ok");
        assert_eq!(requests.len(), 2);
    } else {
        let response = result.expect("fail-on-err-off returns raw 503 response");
        assert_eq!(response.status_code(), 503);
        assert_eq!(response.as_slice(), b"raw service response");
        assert_eq!(requests.len(), 1);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_status_exhaustion_preserves_final_response() {
    let configured_retries = if cfg!(feature = "fail-on-err") {
        crate::get_retries() as usize
    } else {
        0
    };
    let mut replies = Vec::with_capacity(configured_retries + 1);
    for _ in 0..configured_retries {
        replies.push(Reply::Response {
            status: 503,
            body: b"intermediate failure",
        });
    }
    replies.push(Reply::Response {
        status: 503,
        body: if cfg!(feature = "fail-on-err") {
            b"final failure"
        } else {
            b"raw service response"
        },
    });

    let server = start_server(replies);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_data(false).await;
    let requests = server.finish().expect("mock server should finish");

    if cfg!(feature = "fail-on-err") {
        assert!(matches!(
            result,
            Err(S3Error::HttpFailWithBody(503, body)) if body == "final failure"
        ));
    } else {
        let response = result.expect("fail-on-err-off returns the raw response");
        assert_eq!(response.status_code(), 503);
        assert_eq!(response.as_slice(), b"raw service response");
    }
    assert_eq!(requests.len(), configured_retries + 1);
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_get_403_is_not_replayed_and_preserves_response() {
    let server = start_server(vec![Reply::Response {
        status: 403,
        body: b"access denied",
    }]);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_data(false).await;
    let requests = server.finish().expect("mock server should finish");

    assert_eq!(requests.len(), 1);
    if cfg!(feature = "fail-on-err") {
        assert!(matches!(
            result,
            Err(S3Error::HttpFailWithBody(403, body)) if body == "access denied"
        ));
    } else {
        let response = result.expect("fail-on-err-off returns raw 403 response");
        assert_eq!(response.status_code(), 403);
        assert_eq!(response.as_slice(), b"access denied");
    }
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_response_status_retries_safe_get_when_errors_are_enabled() {
    let server = start_server(vec![
        Reply::Close,
        Reply::Response {
            status: 200,
            body: b"",
        },
    ]);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_status().await;
    let requests = server.finish().expect("mock server should finish");

    assert_eq!(result.unwrap(), 200);
    assert_eq!(requests.len(), 2);
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_get_replays_after_lost_response() {
    let server = start_server(vec![
        Reply::Close,
        Reply::Response {
            status: 200,
            body: b"recovered",
        },
    ]);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_data(false).await;
    let requests = server.finish().expect("mock server should finish");

    let response = result.expect("safe GET should retry after a dropped response");
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.as_slice(), b"recovered");
    assert_eq!(requests.len(), 2);
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_multipart_part_replays_same_target_and_body() {
    let server = start_server(vec![
        Reply::Close,
        Reply::Response {
            status: 200,
            body: b"",
        },
    ]);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let command = Command::PutObject {
        content: b"same-part-payload",
        content_type: "application/octet-stream",
        custom_headers: None,
        multipart: Some(Multipart::new(2, "retry-upload-id")),
    };
    let request = make_request(&bucket, command).await.unwrap();
    let result = request.response_data(true).await;
    let requests = server.finish().expect("mock server should finish");

    assert_eq!(requests.len(), 2);
    for captured in &requests {
        assert!(captured.line.starts_with("PUT "));
        assert!(captured.line.contains("partNumber=2"));
        assert!(captured.line.contains("uploadId=retry-upload-id"));
        assert_eq!(captured.body, b"same-part-payload");
    }
    assert!(result.is_ok());
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_mutations_are_not_replayed_after_lost_response() {
    use crate::serde_types::{CompleteMultipartUploadData, Part};

    let commands = [
        Command::InitiateMultipartUpload {
            content_type: "application/octet-stream",
        },
        Command::CompleteMultipartUpload {
            upload_id: "retry-upload-id",
            data: CompleteMultipartUploadData {
                parts: vec![Part {
                    part_number: 1,
                    etag: "part-etag".to_owned(),
                }],
            },
        },
        Command::CopyObject { from: "/source" },
    ];

    for command in commands {
        let server = start_server(vec![Reply::Close]);
        let bucket = test_bucket(server.endpoint.clone()).await;
        let request = make_request(&bucket, command).await.unwrap();
        let result = request.response_data(false).await;
        let requests = server.finish().expect("mock server should finish");

        assert!(
            result.is_err(),
            "lost mutation response must remain an error"
        );
        assert_eq!(requests.len(), 1, "mutation should make one attempt");
        assert!(!requests[0].line.is_empty());
    }
}

#[maybe_async::test(
    feature = "sync",
    async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    async(
        all(not(feature = "sync"), feature = "with-async-std"),
        async_std::test
    )
)]
async fn request_retry_permanent_error_body_read_failure_is_not_replayed() {
    // Invalid chunk framing reliably exercises the client's body-read path;
    // some backends accept a short Content-Length body as a partial response.
    let server = start_server(vec![Reply::MalformedChunkedResponse {
        status: 403,
        body: b"ZZ\r\n<Error/>\r\n0\r\n\r\n",
    }]);
    let bucket = test_bucket(server.endpoint.clone()).await;
    let request = make_request(&bucket, Command::GetObject).await.unwrap();
    let result = request.response_data(false).await;
    let requests = server.finish().expect("mock server should finish");
    assert_eq!(requests.len(), 1);
    #[cfg(feature = "with-tokio")]
    assert!(matches!(result, Err(S3Error::Reqwest(_))));
    #[cfg(feature = "with-async-std")]
    assert!(matches!(result, Err(S3Error::Surf(_))));
    #[cfg(feature = "sync")]
    assert!(matches!(result, Err(S3Error::Atto(_))));
}
