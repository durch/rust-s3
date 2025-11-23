extern crate base64;
extern crate md5;

use attohttpc::header::HeaderName;
use std::time::Duration;

use crate::error::S3Error;
use std::borrow::Cow;

use crate::request::backend::{BackendRequestBody, SyncService};

fn http_request_to_atto_request(
    request: http::Request<BackendRequestBody<'_>>,
    request_timeout: Option<Duration>,
) -> Result<attohttpc::RequestBuilder<attohttpc::body::Bytes<Cow<'_, [u8]>>>, S3Error> {
    let mut session = attohttpc::Session::new();

    for (name, value) in request.headers().iter() {
        session.header(HeaderName::from_bytes(name.as_ref())?, value.to_str()?);
    }

    if let Some(timeout) = request_timeout {
        session.timeout(timeout)
    }

    let url = format!("{}", request.uri());
    let builder = match *request.method() {
        http::Method::GET => session.get(url),
        http::Method::DELETE => session.delete(url),
        http::Method::PUT => session.put(url),
        http::Method::POST => session.post(url),
        http::Method::HEAD => session.head(url),
        _ => {
            return Err(S3Error::HttpFailWithBody(405, "".into()));
        }
    };

    Ok(builder.bytes(request.body().clone()))
}

impl SyncService<http::Request<BackendRequestBody<'_>>> for AttoBackend {
    type Response = http::Response<attohttpc::ResponseReader>;
    type Error = S3Error;

    fn call(
        &mut self,
        request: http::Request<BackendRequestBody<'_>>,
    ) -> Result<http::Response<attohttpc::ResponseReader>, S3Error> {
        let response = http_request_to_atto_request(request, self.request_timeout)?.send()?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text()?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        let (status, headers, body) = response.split();
        let mut builder =
            http::Response::builder().status(http::StatusCode::from_u16(status.into())?);
        *builder.headers_mut().unwrap() = headers;

        Ok(builder.body(body)?)
    }
}

#[derive(Clone, Debug)]
pub struct AttoBackend {
    request_timeout: Option<Duration>,
}

impl Default for AttoBackend {
    fn default() -> Self {
        Self {
            request_timeout: crate::request::backend::DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

impl AttoBackend {
    pub fn with_request_timeout(&self, request_timeout: Option<Duration>) -> Result<Self, S3Error> {
        Ok(Self { request_timeout })
    }

    pub fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build() {
        let http_request = http::Request::builder()
            .uri("https://example.com/foo?bar=1")
            .method(http::Method::POST)
            .header("h1", "v1")
            .header("h2", "v2")
            .body(b"sneaky".into())
            .unwrap();

        let mut r = http_request_to_atto_request(http_request, None).unwrap();

        assert_eq!(r.inspect().method(), http::Method::POST);
        assert_eq!(r.inspect().url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.headers().get("h1").unwrap(), "v1");
        assert_eq!(r.headers().get("h2").unwrap(), "v2");
        let mut i = r.inspect();
        let body = &i.body().0;
        assert_eq!(&**body, b"sneaky");
    }
}
