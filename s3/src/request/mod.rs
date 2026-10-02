#[cfg(feature = "with-async-std")]
pub mod async_std_backend;
#[cfg(feature = "sync")]
pub mod blocking;
pub mod request_trait;
#[cfg(feature = "with-tokio")]
pub mod tokio_backend;

pub use request_trait::*;

#[cfg(test)]
mod retry_tests;

use crate::command::Command;
use crate::error::S3Error;

const RETRYABLE_HTTP_STATUSES: [u16; 6] = [408, 429, 500, 502, 503, 504];

pub(crate) struct RequestAttemptError {
    pub(crate) error: S3Error,
    retryable: Option<bool>,
}

impl RequestAttemptError {
    pub(crate) fn do_not_retry(error: S3Error) -> Self {
        Self {
            error,
            retryable: Some(false),
        }
    }

    #[cfg(feature = "with-async-std")]
    pub(crate) fn surf_send_error(error: surf::Error) -> Self {
        // Surf's transport stack can report connection-close failures without
        // retaining an io::Error source. At this boundary errors come only
        // from send(); replay remains restricted to safe operations.
        Self {
            error: S3Error::Surf(error.to_string()),
            retryable: Some(true),
        }
    }

    fn should_retry(&self, command: &Command<'_>) -> bool {
        command.is_retry_safe()
            && self
                .retryable
                .unwrap_or_else(|| retryable_request_error(command, &self.error))
    }
}

impl From<S3Error> for RequestAttemptError {
    fn from(error: S3Error) -> Self {
        Self {
            error,
            retryable: None,
        }
    }
}

#[cfg(feature = "with-tokio")]
impl From<reqwest::Error> for RequestAttemptError {
    fn from(error: reqwest::Error) -> Self {
        S3Error::Reqwest(error).into()
    }
}

#[cfg(feature = "sync")]
impl From<attohttpc::Error> for RequestAttemptError {
    fn from(error: attohttpc::Error) -> Self {
        S3Error::Atto(error).into()
    }
}

fn retryable_request_error(command: &Command<'_>, error: &S3Error) -> bool {
    if !command.is_retry_safe() {
        return false;
    }

    match error {
        S3Error::HttpFailWithBody(status, _) => RETRYABLE_HTTP_STATUSES.contains(status),
        #[cfg(feature = "with-tokio")]
        S3Error::Reqwest(error) => {
            error.is_connect()
                || error.is_timeout()
                || (error.is_request() && !error.is_builder() && error.url().is_some())
                || reqwest_has_transient_io(error)
        }
        #[cfg(feature = "with-async-std")]
        S3Error::Surf(_) => false,
        #[cfg(feature = "sync")]
        S3Error::Atto(error) => match error.kind() {
            attohttpc::ErrorKind::Io(error) => is_transient_io_kind(error.kind()),
            _ => false,
        },
        _ => false,
    }
}

#[cfg(any(feature = "with-tokio", feature = "sync"))]
fn is_transient_io_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::NetworkUnreachable
            | std::io::ErrorKind::HostUnreachable
            | std::io::ErrorKind::AddrNotAvailable
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
    )
}

#[cfg(feature = "with-tokio")]
fn reqwest_has_transient_io(error: &reqwest::Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
            return is_transient_io_kind(io_error.kind());
        }
        source = current.source();
    }
    false
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub(crate) async fn retry_request<T, F, Fut>(
    command: &Command<'_>,
    mut attempt: F,
) -> Result<T, S3Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RequestAttemptError>>,
{
    let mut retry_count = 0u64;
    let max_retries = crate::get_retries() as u64;

    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) if retry_count < max_retries && error.should_retry(command) => {
                retry_count += 1;
                log::warn!("Retrying safe S3 request after transient failure");
                let delay = std::time::Duration::from_secs(retry_count.pow(2));
                #[cfg(feature = "with-tokio")]
                tokio::time::sleep(delay).await;
                #[cfg(all(not(feature = "with-tokio"), feature = "with-async-std"))]
                async_std::task::sleep(delay).await;
            }
            Err(error) => return Err(error.error),
        }
    }
}

#[cfg(feature = "sync")]
pub(crate) fn retry_request_sync<T, F>(command: &Command<'_>, mut attempt: F) -> Result<T, S3Error>
where
    F: FnMut() -> Result<T, RequestAttemptError>,
{
    let mut retry_count = 0u64;
    let max_retries = crate::get_retries() as u64;

    loop {
        match attempt() {
            Ok(value) => return Ok(value),
            Err(error) if retry_count < max_retries && error.should_retry(command) => {
                retry_count += 1;
                log::warn!("Retrying safe S3 request after transient failure");
                std::thread::sleep(std::time::Duration::from_secs(retry_count.pow(2)));
            }
            Err(error) => return Err(error.error),
        }
    }
}

#[cfg(test)]
mod retry_policy_tests {
    use super::*;

    #[test]
    fn request_retry_policy_requires_safe_operation_and_transient_status() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(retryable_request_error(
                &Command::GetObject,
                &S3Error::HttpFailWithBody(status, "retry".to_owned())
            ));
        }
        for status in [200, 400, 401, 403, 404, 5000, 501] {
            assert!(!retryable_request_error(
                &Command::GetObject,
                &S3Error::HttpFailWithBody(status, "permanent".to_owned())
            ));
        }
        assert!(!retryable_request_error(
            &Command::DeleteObject,
            &S3Error::HttpFailWithBody(503, "mutation".to_owned())
        ));
        assert!(!retryable_request_error(
            &Command::InitiateMultipartUpload {
                content_type: "application/octet-stream",
            },
            &S3Error::HttpFailWithBody(503, "mutation".to_owned())
        ));
        assert!(!retryable_request_error(
            &Command::UploadPart {
                part_number: 1,
                content: b"part",
                upload_id: "upload-id",
            },
            &S3Error::HttpFailWithBody(503, "legacy command".to_owned())
        ));
    }

    #[test]
    fn request_retry_upload_part_is_safe_but_ordinary_put_is_not() {
        use crate::command::Multipart;

        let part = Command::PutObject {
            content: b"part",
            content_type: "application/octet-stream",
            custom_headers: None,
            multipart: Some(Multipart::new(1, "upload-id")),
        };
        let ordinary_put = Command::PutObject {
            content: b"object",
            content_type: "application/octet-stream",
            custom_headers: None,
            multipart: None,
        };

        assert!(part.is_retry_safe());
        assert!(!ordinary_put.is_retry_safe());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn request_retry_atto_policy_only_accepts_transport_io_errors() {
        let transport = attohttpc::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        let invalid_url = attohttpc::Error::from(attohttpc::ErrorKind::InvalidBaseUrl);

        assert!(retryable_request_error(
            &Command::GetObject,
            &S3Error::Atto(transport)
        ));
        assert!(!retryable_request_error(
            &Command::GetObject,
            &S3Error::Atto(invalid_url)
        ));
    }

    #[cfg(feature = "with-tokio")]
    #[tokio::test]
    async fn request_retry_safe_request_retries_transient_status_once() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = retry_request(&Command::GetObject, || {
            let current = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if current == 0 {
                    Err(S3Error::HttpFailWithBody(503, "temporary".to_owned()).into())
                } else {
                    Ok(200)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(result, 200);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[cfg(feature = "with-tokio")]
    #[tokio::test]
    async fn request_retry_mutation_and_body_read_error_are_not_replayed() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result: Result<(), S3Error> = retry_request(&Command::DeleteObject, || {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), RequestAttemptError>(
                    S3Error::HttpFailWithBody(503, "service error".to_owned()).into(),
                )
            }
        })
        .await;
        assert!(
            matches!(result, Err(S3Error::HttpFailWithBody(503, body)) if body == "service error")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let attempts = AtomicUsize::new(0);
        let result: Result<(), S3Error> = retry_request(&Command::GetObject, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), RequestAttemptError>(RequestAttemptError::do_not_retry(S3Error::Io(
                    std::io::Error::other("original body read diagnostic"),
                )))
            }
        })
        .await;
        assert!(
            matches!(result, Err(S3Error::Io(error)) if error.to_string() == "original body read diagnostic")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "with-async-std")]
    #[async_std::test]
    async fn request_retry_safe_request_retries_transient_status_once() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = retry_request(&Command::GetObject, || {
            let current = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if current == 0 {
                    Err(S3Error::HttpFailWithBody(503, "temporary".to_owned()).into())
                } else {
                    Ok(200)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(result, 200);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[cfg(feature = "with-async-std")]
    #[async_std::test]
    async fn request_retry_mutation_and_body_read_error_are_not_replayed() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result: Result<(), S3Error> = retry_request(&Command::DeleteObject, || {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), RequestAttemptError>(
                    S3Error::HttpFailWithBody(503, "service error".to_owned()).into(),
                )
            }
        })
        .await;
        assert!(
            matches!(result, Err(S3Error::HttpFailWithBody(503, body)) if body == "service error")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let attempts = AtomicUsize::new(0);
        let result: Result<(), S3Error> = retry_request(&Command::GetObject, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), RequestAttemptError>(RequestAttemptError::do_not_retry(S3Error::Io(
                    std::io::Error::other("original body read diagnostic"),
                )))
            }
        })
        .await;
        assert!(
            matches!(result, Err(S3Error::Io(error)) if error.to_string() == "original body read diagnostic")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn request_retry_mutation_does_not_retry_transient_status() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = AtomicUsize::new(0);
        let result: Result<(), S3Error> = retry_request_sync(&Command::DeleteObject, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(S3Error::HttpFailWithBody(503, "do not replay".to_owned()).into())
        });

        assert!(
            matches!(result, Err(S3Error::HttpFailWithBody(503, body)) if body == "do not replay")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
