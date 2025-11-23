use std::borrow::Cow;
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

pub type BackendRequestBody<'a> = Cow<'a, [u8]>;

/// A simplified version of tower_service::Service without async
#[cfg(feature = "sync")]
pub trait SyncService<R> {
    type Response;
    type Error;

    fn call(&mut self, _: R) -> Result<Self::Response, Self::Error>;
}
