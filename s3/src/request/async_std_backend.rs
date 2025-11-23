use bytes::Bytes;
use futures_util::AsyncBufRead as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower_service::Service;

use crate::error::S3Error;

use crate::request::backend::BackendRequestBody;

use http_body::Frame;
use surf::http::Method;
use surf::http::headers::{HeaderName, HeaderValue};

fn http_request_to_surf_request(
    request: http::Request<BackendRequestBody<'_>>,
) -> Result<surf::RequestBuilder, S3Error> {
    let url = format!("{}", request.uri()).parse()?;
    let mut builder = match *request.method() {
        http::Method::GET => surf::Request::builder(Method::Get, url),
        http::Method::DELETE => surf::Request::builder(Method::Delete, url),
        http::Method::PUT => surf::Request::builder(Method::Put, url),
        http::Method::POST => surf::Request::builder(Method::Post, url),
        http::Method::HEAD => surf::Request::builder(Method::Head, url),
        ref m => surf::Request::builder(
            m.as_str()
                .parse()
                .map_err(|e: surf::Error| S3Error::Surf(e.to_string()))?,
            url,
        ),
    }
    .body(request.body().clone().into_owned());

    for (name, value) in request.headers().iter() {
        builder = builder.header(
            HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                .expect("Could not parse heaeder name"),
            HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                .expect("Could not parse header value"),
        );
    }

    Ok(builder)
}

pub struct SurfBody(surf::Body);

impl http_body::Body for SurfBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        let mut inner = Pin::new(&mut self.0);
        match inner.as_mut().poll_fill_buf(cx) {
            Poll::Ready(Ok(sliceu8)) => {
                if sliceu8.is_empty() {
                    Poll::Ready(None)
                } else {
                    let len = sliceu8.len();
                    let frame = Frame::data(Bytes::copy_from_slice(sliceu8));
                    inner.as_mut().consume(len);
                    Poll::Ready(Some(Ok(frame)))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Service<http::Request<BackendRequestBody<'_>>> for SurfBackend {
    type Response = http::Response<SurfBody>;
    type Error = S3Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S3Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<BackendRequestBody<'_>>) -> Self::Future {
        match http_request_to_surf_request(request) {
            Ok(request) => {
                let fut = request.send();
                Box::pin(async move {
                    let mut response = fut.await.map_err(|e| S3Error::Surf(e.to_string()))?;

                    if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                        return Err(S3Error::HttpFail);
                    }

                    let mut builder = http::Response::builder()
                        .status(http::StatusCode::from_u16(response.status().into())?);
                    for (name, values) in response.iter() {
                        for value in values {
                            builder = builder.header(name.as_str(), value.as_str());
                        }
                    }
                    Ok(builder.body(SurfBody(response.take_body()))?)
                })
            }
            Err(e) => Box::pin(std::future::ready(Err(e))),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SurfBackend {}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn test_build() {
        let http_request = http::Request::builder()
            .uri("https://example.com/foo?bar=1")
            .method(http::Method::POST)
            .header("h1", "v1")
            .header("h2", "v2")
            .body(b"sneaky".into())
            .unwrap();

        let mut r = http_request_to_surf_request(http_request).unwrap().build();

        assert_eq!(r.method(), Method::Post);
        assert_eq!(r.url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.header("h1").unwrap(), "v1");
        assert_eq!(r.header("h2").unwrap(), "v2");
        let body = r.take_body().into_bytes().await.unwrap();
        assert_eq!(body.as_slice(), b"sneaky");
    }
}
