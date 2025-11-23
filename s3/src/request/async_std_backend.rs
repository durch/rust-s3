use bytes::Bytes;
use futures_util::AsyncBufRead as _;
use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::S3Error;

use crate::request::Request;

use http_body::Frame;
use maybe_async::maybe_async;
use surf::http::Method;
use surf::http::headers::{HeaderName, HeaderValue};

// Temporary structure for making a request
pub struct SurfRequest<'a> {
    request: http::Request<Cow<'a, [u8]>>,
    pub sync: bool,
}

impl SurfRequest<'_> {
    fn build(&self) -> Result<surf::RequestBuilder, S3Error> {
        let url = format!("{}", self.request.uri()).parse()?;
        let mut request = match *self.request.method() {
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
        .body(self.request.body().clone().into_owned());

        for (name, value) in self.request.headers().iter() {
            request = request.header(
                HeaderName::from_bytes(AsRef::<[u8]>::as_ref(&name).to_vec())
                    .expect("Could not parse heaeder name"),
                HeaderValue::from_bytes(AsRef::<[u8]>::as_ref(&value).to_vec())
                    .expect("Could not parse header value"),
            );
        }

        Ok(request)
    }
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

#[maybe_async]
impl<'a> Request for SurfRequest<'a> {
    type ResponseBody = SurfBody;

    async fn response(&self) -> Result<http::Response<SurfBody>, S3Error> {
        let mut response = self
            .build()?
            .send()
            .await
            .map_err(|e| S3Error::Surf(e.to_string()))?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            return Err(S3Error::HttpFail);
        }

        let mut builder =
            http::Response::builder().status(http::StatusCode::from_u16(response.status().into())?);
        for (name, values) in response.iter() {
            for value in values {
                builder = builder.header(name.as_str(), value.as_str());
            }
        }
        Ok(builder.body(SurfBody(response.take_body()))?)
    }
}

impl SurfBackend {
    pub(crate) fn request<'a>(&self, request: http::Request<Cow<'a, [u8]>>) -> SurfRequest<'a> {
        SurfRequest {
            request,
            sync: false,
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

        let mut r = SurfBackend::default()
            .request(http_request)
            .build()
            .unwrap()
            .build();

        assert_eq!(r.method(), Method::Post);
        assert_eq!(r.url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.header("h1").unwrap(), "v1");
        assert_eq!(r.header("h2").unwrap(), "v2");
        let body = r.take_body().into_bytes().await.unwrap();
        assert_eq!(body.as_slice(), b"sneaky");
    }
}
