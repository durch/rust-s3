extern crate base64;
extern crate md5;

use std::io;
use std::io::Write;

use attohttpc::header::HeaderName;

use crate::bucket::Bucket;
use crate::error::S3Error;
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::HashMap;

use crate::request::{Request, ResponseData};

// Temporary structure for making a request
pub struct AttoRequest<'a> {
    request: http::Request<Cow<'a, [u8]>>,
    request_timeout: Option<std::time::Duration>,
    pub sync: bool,
}

impl AttoRequest<'_> {
    fn build(
        &self,
    ) -> Result<attohttpc::RequestBuilder<attohttpc::body::Bytes<Cow<'_, [u8]>>>, S3Error> {
        let mut session = attohttpc::Session::new();

        for (name, value) in self.request.headers().iter() {
            session.header(HeaderName::from_bytes(name.as_ref())?, value.to_str()?);
        }

        if let Some(timeout) = self.request_timeout {
            session.timeout(timeout)
        }

        let url = format!("{}", self.request.uri());
        let request = match *self.request.method() {
            http::Method::GET => session.get(url),
            http::Method::DELETE => session.delete(url),
            http::Method::PUT => session.put(url),
            http::Method::POST => session.post(url),
            http::Method::HEAD => session.head(url),
            _ => {
                return Err(S3Error::HttpFailWithBody(405, "".into()));
            }
        };

        Ok(request.bytes(self.request.body().clone()))
    }
}

impl<'a> Request for AttoRequest<'a> {
    type Response = attohttpc::Response;
    type HeaderMap = attohttpc::header::HeaderMap;

    fn response(&self) -> Result<Self::Response, S3Error> {
        let response = self.build()?.send()?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text()?;
            return Err(S3Error::HttpFailWithBody(status, text));
        }

        Ok(response)
    }

    fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let response = crate::retry! {self.response()}?;
        let status_code = response.status().as_u16();

        let response_headers = response
            .headers()
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
            if let Some(etag) = response.headers().get("ETag") {
                Bytes::from(etag.to_str()?.to_string())
            } else {
                Bytes::from("")
            }
        } else {
            // HEAD requests don't have a response body
            if *self.request.method() == http::Method::HEAD {
                Bytes::from("")
            } else {
                Bytes::from(response.bytes()?)
            }
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    fn response_data_to_writer<T: Write + ?Sized>(&self, writer: &mut T) -> Result<u16, S3Error> {
        let mut response = crate::retry! {self.response()}?;

        let status_code = response.status();
        io::copy(&mut response, writer)?;

        Ok(status_code.as_u16())
    }

    fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let response = crate::retry! {self.response()}?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }
}

impl<'a> AttoRequest<'a> {
    pub fn new(request: http::Request<Cow<'a, [u8]>>, bucket: &Bucket) -> Result<Self, S3Error> {
        Ok(Self {
            request,
            request_timeout: bucket.request_timeout,
            sync: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::Bucket;
    use awscreds::Credentials;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[test]
    fn test_build() {
        let http_request = http::Request::builder()
            .uri("https://example.com/foo?bar=1")
            .method(http::Method::POST)
            .header("h1", "v1")
            .header("h2", "v2")
            .body(b"sneaky".into())
            .unwrap();
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials()).unwrap();

        let req = AttoRequest::new(http_request, &bucket).unwrap();
        let mut r = req.build().unwrap();

        assert_eq!(r.inspect().method(), http::Method::POST);
        assert_eq!(r.inspect().url().as_str(), "https://example.com/foo?bar=1");
        assert_eq!(r.headers().get("h1").unwrap(), "v1");
        assert_eq!(r.headers().get("h2").unwrap(), "v2");
        let mut i = r.inspect();
        let body = &i.body().0;
        assert_eq!(&**body, b"sneaky");
    }
}
