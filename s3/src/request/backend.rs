use std::time::Duration;

#[cfg(feature = "with-async-std")]
pub(crate) use crate::request::async_std_backend::SurfBackend as DefaultBackend;
#[cfg(feature = "sync")]
pub(crate) use crate::request::blocking::AttoBackend as DefaultBackend;
#[cfg(feature = "with-tokio")]
pub(crate) use crate::request::tokio_backend::ReqwestBackend as DefaultBackend;

/// Default request timeout. Override with s3::Bucket::with_request_timeout.
///
/// For backward compatibility, only AttoBackend uses this. ReqwestBackend
/// supports a timeout but none is set by default.
pub const DEFAULT_REQUEST_TIMEOUT: Option<Duration> = Some(Duration::from_secs(60));
