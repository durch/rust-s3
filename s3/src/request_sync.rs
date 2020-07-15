extern crate base64;
extern crate md5;

use std::collections::HashMap;
use std::io::Write;

use super::bucket::Bucket;
use super::command::Command;
use chrono::{DateTime, Utc};

use crate::{Result, S3Error};
use std::convert::From;

use once_cell::sync::Lazy;

use attohttpc::header::{HeaderMap, HeaderName, HeaderValue};
use attohttpc::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST};
use attohttpc::{Response, Session};

use crate::command::Method;
use crate::request_utils::{Request, url, host_header, authorization, sha256, content_length, content_type, long_date};

/// Collection of HTTP headers sent to S3 service, in key/value format.
pub type Headers = HashMap<String, String>;

/// Collection of HTTP query parameters sent to S3 service, in key/value
/// format.
pub type Query = HashMap<String, String>;

fn into_hash_map(h: &HeaderMap) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for (k, v) in h.iter() {
        out.insert(k.to_string(), v.to_str()?.to_string());
    }
    Ok(out)
}

static SESSION: Lazy<Session> = Lazy::new(|| {
    // if cfg!(feature = "no-verify-ssl") {
    //     let mut session = Session::new();
    //     session.danger_accept_invalid_certs(true);
    //     session.danger_accept_invalid_hostnames(true);
    //     session
    // } else {
    Session::new()
    // }
});

// Temporary structure for making a request
pub struct RequestSync<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: DateTime<Utc>,
    pub sync: bool,
}

impl Request for RequestSync<'_> {
    fn command(&self) -> &Command<'_> {
        &self.command
    }
    fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }
    fn bucket(&self) -> &Bucket {
        self.bucket
    }
    fn path(&self) -> &str {
        self.path
    }
}

impl<'a> RequestSync<'a> {
    pub fn response_data(&self) -> Result<(Vec<u8>, u16)> {
        let response = self.response()?;
        let status_code = response.status().as_u16();
        let body = match response.bytes() {
            Ok(body) => body,
            Err(e) => return Err(S3Error::from(format!("{}", e).as_ref())),
        };
        Ok((body.to_vec(), status_code))
    }

    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> RequestSync<'b> {
        RequestSync {
            bucket,
            path,
            command,
            datetime: Utc::now(),
            sync: false,
        }
    }

    fn headers(&self) -> Result<HeaderMap> {
        // Generate this once, but it's used in more than one place.
        let sha256 = sha256(self);

        // Start with extra_headers, that way our headers replace anything with
        // the same name.

        let mut headers = HeaderMap::new();

        for (k, v) in self.bucket.extra_headers.iter() {
            headers.insert(
                match HeaderName::from_bytes(k.as_bytes()) {
                    Ok(name) => name,
                    Err(e) => {
                        return Err(S3Error::from(
                            format!("Could not parse {} to HeaderName.\n {}", k, e).as_ref(),
                        ))
                    }
                },
                match HeaderValue::from_bytes(v.as_bytes()) {
                    Ok(value) => value,
                    Err(e) => {
                        return Err(S3Error::from(
                            format!("Could not parse {} to HeaderValue.\n {}", v, e).as_ref(),
                        ))
                    }
                },
            );
        }

        let host_header = host_header(self);

        headers.insert(HOST, HeaderValue::from_str(&host_header)?);

        match self.command {
            Command::ListBucket { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}
            _ => {
                headers.insert(
                    CONTENT_LENGTH,
                    match content_length(self).to_string().parse() {
                        Ok(content_length) => content_length,
                        Err(_) => {
                            return Err(S3Error::from(
                                format!(
                                    "Could not parse CONTENT_LENGTH header value {}",
                                    content_length(self)
                                )
                                .as_ref(),
                            ))
                        }
                    },
                );
                headers.insert(
                    CONTENT_TYPE,
                    match content_type(self).parse() {
                        Ok(content_type) => content_type,
                        Err(_) => {
                            return Err(S3Error::from(
                                format!(
                                    "Could not parse CONTENT_TYPE header value {}",
                                    content_type(self)
                                )
                                .as_ref(),
                            ))
                        }
                    },
                );
            }
        }
        headers.insert(
            "X-Amz-Content-Sha256",
            match sha256.parse() {
                Ok(value) => value,
                Err(_) => {
                    return Err(S3Error::from(
                        format!(
                            "Could not parse X-Amz-Content-Sha256 header value {}",
                            sha256
                        )
                        .as_ref(),
                    ))
                }
            },
        );
        headers.insert(
            "X-Amz-Date",
            match long_date(self).parse() {
                Ok(value) => value,
                Err(_) => {
                    return Err(S3Error::from(
                        format!(
                            "Could not parse X-Amz-Date header value {}",
                            long_date(self)
                        )
                        .as_ref(),
                    ))
                }
            },
        );

        if let Some(session_token) = self.bucket.session_token() {
            headers.insert(
                "X-Amz-Security-Token",
                match session_token.parse() {
                    Ok(session_token) => session_token,
                    Err(_) => {
                        return Err(S3Error::from(
                            format!(
                                "Could not parse X-Amz-Security-Token header value {}",
                                session_token
                            )
                            .as_ref(),
                        ))
                    }
                },
            );
        } else if let Some(security_token) = self.bucket.security_token() {
            headers.insert(
                "X-Amz-Security-Token",
                match security_token.parse() {
                    Ok(security_token) => security_token,
                    Err(_) => {
                        return Err(S3Error::from(
                            format!(
                                "Could not parse X-Amz-Security-Token header value {}",
                                security_token
                            )
                            .as_ref(),
                        ))
                    }
                },
            );
        }

        if let Command::PutObjectTagging { tags } = self.command {
            let digest = md5::compute(tags);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5", hash.parse()?);
        } else if let Command::PutObject { content, .. } = self.command {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5", hash.parse()?);
        } else if let Command::GetObject {} = self.command {
            headers.insert(ACCEPT, HeaderValue::from_str("application/octet-stream")?);
            // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket.secret_key().is_some() {
            let authorization = authorization(self, &into_hash_map(&headers)?)?;
            headers.insert(
                AUTHORIZATION,
                match authorization.parse() {
                    Ok(authorization) => authorization,
                    Err(_) => {
                        return Err(S3Error::from(
                            format!(
                                "Could not parse AUTHORIZATION header value {}",
                                authorization
                            )
                            .as_ref(),
                        ))
                    }
                },
            );
        }

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(
            DATE,
            match self.datetime.to_rfc2822().parse() {
                Ok(date) => date,
                Err(_) => {
                    return Err(S3Error::from(
                        format!(
                            "Could not parse DATE header value {}",
                            self.datetime.to_rfc2822()
                        )
                        .as_ref(),
                    ))
                }
            },
        );

        Ok(headers)
    }

    // pub fn response_data(&self) -> Result<(Vec<u8>, u16)> {
    //     Ok(futures::executor::block_on(self.response_data_future())?)
    // }

    // pub fn response_data_to_writer<T: Write>(&self, writer: &mut T) -> Result<u16> {
    //     Ok(futures::executor::block_on(self.response_data_to_writer_future(writer))?)
    // }

    pub fn response_data_to_writer<'b, T: Write>(&self, writer: &'b mut T) -> Result<u16> {
        let response = self.response()?;

        let status_code = response.status();
        match response.write_to(writer) {
            Ok(_) => {}
            Err(e) => return Err(S3Error::from(format!("{}", e).as_ref())),
        }

        Ok(status_code.as_u16())
    }

    pub fn response(&self) -> Result<Response> {
        // Build headers
        let headers = match self.headers() {
            Ok(headers) => headers,
            Err(e) => return Err(e),
        };

        // Get owned content to pass to reqwest
        let content = if let Command::PutObject { content, .. } = self.command {
            Vec::from(content)
        } else if let Command::PutObjectTagging { tags } = self.command {
            Vec::from(tags)
        } else {
            Vec::new()
        };

        let mut request = match self.command.http_verb() {
            Method::Get => SESSION.get(url(self).as_str()),
            Method::Put => SESSION.put(url(self).as_str()),
            Method::Delete => SESSION.delete(url(self).as_str()),
        };

        for (name, value) in headers.iter() {
            request = request.header_append(name, value);
        }

        let request = request.bytes(content.as_slice());

        let response = match request.send() {
            Ok(response) => response,
            Err(e) => return Err(S3Error::from(format!("{}", e).as_ref())),
        };

        if cfg!(feature = "fail-on-err") && response.status().as_u16() >= 400 {
            return Err(S3Error::from(
                format!(
                    "Request failed with code {}\n{}",
                    response.status().as_u16(),
                    match response.text() {
                        Ok(text) => text,
                        Err(e) => format!("{}", e),
                    }
                )
                .as_str(),
            ));
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request_sync::RequestSync;
    use crate::Result;
    use crate::request_utils::url;
    use awscreds::Credentials;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[test]
    fn url_uses_https_by_default() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = RequestSync::new(&bucket, path, Command::GetObject);

        assert_eq!(url(&request).scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_https_by_default_path_style() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new_with_path_style("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = RequestSync::new(&bucket, path, Command::GetObject);

        assert_eq!(url(&request).scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = RequestSync::new(&bucket, path, Command::GetObject);

        assert_eq!(url(&request).scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new_with_path_style("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = RequestSync::new(&bucket, path, Command::GetObject);

        assert_eq!(url(&request).scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }
}
