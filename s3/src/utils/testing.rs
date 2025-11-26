#[derive(Clone)]
pub struct AlwaysFailBackend;

#[cfg(not(feature = "sync"))]
impl<R> tower_service::Service<R> for AlwaysFailBackend {
    type Response = http::Response<http_body_util::Empty<&'static [u8]>>;
    type Error = http::Error;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: R) -> Self::Future {
        std::future::ready(
            http::Response::builder()
                .status(http::StatusCode::IM_A_TEAPOT)
                .body(http_body_util::Empty::new()),
        )
    }
}

#[cfg(feature = "sync")]
impl<R> crate::request::backend::SyncService<R> for AlwaysFailBackend {
    type Response = http::Response<&'static [u8]>;
    type Error = http::Error;

    fn call(&mut self, _: R) -> Result<Self::Response, Self::Error> {
        http::Response::builder()
            .status(http::StatusCode::IM_A_TEAPOT)
            .body(b"")
    }
}
