extern crate base64;
extern crate md5;

use std::io::Write;

use attohttpc::{header::HeaderName, Session};

use super::bucket::Bucket;
use super::command::Command;
use crate::error::S3Error;
use time::OffsetDateTime;

use crate::command::HttpMethod;
use crate::request_trait::Request;

// Temporary structure for making a request
pub struct AttoRequest<'a> {
    pub session: &'a Session,
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
}

impl<'a> Request for AttoRequest<'a> {
    type Response = attohttpc::Response;
    type HeaderMap = attohttpc::header::HeaderMap;

    fn datetime(&self) -> OffsetDateTime {
        self.datetime
    }

    fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }

    fn command(&self) -> Command {
        self.command.clone()
    }

    fn path(&self) -> String {
        self.path.to_string()
    }

    fn response(&self) -> Result<Self::Response, S3Error> {
        // Build headers
        let headers = match self.headers() {
            Ok(headers) => headers,
            Err(e) => return Err(e),
        };

        let mut request = match self.command.http_verb() {
            HttpMethod::Get => self.session.get(self.url()),
            HttpMethod::Delete => self.session.delete(self.url()),
            HttpMethod::Put => self.session.put(self.url()),
            HttpMethod::Post => self.session.post(self.url()),
            HttpMethod::Head => self.session.head(self.url()),
        };

        for (name, value) in headers.iter() {
            request = request.header(HeaderName::from_bytes(name.as_ref()).unwrap(), value);
        }

        if let Some(timeout) = self.bucket.request_timeout {
            request = request.timeout(timeout);
        }

        let response = request.bytes(&self.request_body()).send()?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text()?;
            return Err(S3Error::Http(status, text));
        }

        Ok(response)
    }

    fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16), S3Error> {
        let response = self.response()?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        let etag_header = headers.get("ETag");
        let body = response.bytes()?;
        let mut body_vec = Vec::new();
        body_vec.extend_from_slice(&body[..]);
        if etag {
            if let Some(etag) = etag_header {
                body_vec = etag.to_str()?.as_bytes().to_vec();
            }
        }
        Ok((body_vec, status_code))
    }

    fn response_data_to_writer<T: Write>(&self, writer: &mut T) -> Result<u16, S3Error> {
        let response = self.response()?;

        let status_code = response.status();
        let stream = response.bytes()?;

        writer.write_all(&stream)?;

        Ok(status_code.as_u16())
    }

    fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let response = self.response()?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }
}

#[cfg(test)]
mod tests {
    use crate::blocking::AttoRequest;
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::error::S3Error;
    use crate::request_trait::Request;
    use awscreds::Credentials;
    use time::OffsetDateTime;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[test]
    fn url_uses_https_by_default() -> Result<(), S3Error> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let session = attohttpc::Session::new();
        let request = AttoRequest {
            session: &session,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: true,
        };

        assert_eq!(request.url().scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_https_by_default_path_style() -> Result<(), S3Error> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-first/path";
        let session = attohttpc::Session::new();
        let request = AttoRequest {
            session: &session,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: true,
        };

        assert_eq!(request.url().scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined() -> Result<(), S3Error> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let session = attohttpc::Session::new();
        let request = AttoRequest {
            session: &session,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: true,
        };

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<(), S3Error> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?.with_path_style();
        let path = "/my-second/path";
        let session = attohttpc::Session::new();
        let request = AttoRequest {
            session: &session,
            bucket: &bucket,
            path,
            command: Command::GetObject,
            datetime: OffsetDateTime::now_utc(),
            sync: true,
        };

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }
}
