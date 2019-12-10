extern crate base64;
extern crate md5;

use std::collections::HashMap;
use std::io::{Read, Write};

use bucket::Bucket;
use chrono::{DateTime, Utc};
use command::Command;
use hmac::Mac;
use reqwest::async;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use url::Url;

use futures::prelude::*;
use tokio::runtime::current_thread::Runtime;

use signing;

use error::{Result, S3Error};
use reqwest::async::Response;
use EMPTY_PAYLOAD_SHA;
use LONG_DATE;

/// Collection of HTTP headers sent to S3 service, in key/value format.
pub type Headers = HashMap<String, String>;

/// Collection of HTTP query parameters sent to S3 service, in key/value
/// format.
pub type Query = HashMap<String, String>;

// Temporary structure for making a request
pub struct Request<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: DateTime<Utc>,
    pub async: bool,
}

impl<'a> Request<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Request<'b> {
        Request {
            bucket,
            path,
            command,
            datetime: Utc::now(),
            async: false,
        }
    }

    // TODO allow for using path style for non dns compliant bucket names
    fn url(&self) -> Url {
        // let mut url_str = match self.command {
        //     Command::ListBucket { .. } => {
        //         format!("{}://{}", self.bucket.scheme(), self.bucket.host())
        //     }
        //     _ => format!("{}://{}", self.bucket.scheme(), self.bucket.self_host()),
        // };
        let mut url_str = format!("{}://{}", self.bucket.scheme(), self.bucket.self_host());
        // if let Command::ListBucket { .. } = self.command {
        //     url_str.push_str("/");
        //     url_str.push_str(&self.bucket.name());
        // }
        if !self.path.starts_with('/') {
            url_str.push_str("/");
        }
        match self.command {
            Command::GetBucketLocation => url_str.push_str(self.path),
            _ => url_str.push_str(&signing::uri_encode(self.path, false)),
        };
        println!("{}", url_str);

        // Since every part of this URL is either pre-encoded or statically
        // generated, there's really no way this should fail.
        let mut url = Url::parse(&url_str).expect("static URL parsing");

        for (key, value) in &self.bucket.extra_query {
            url.query_pairs_mut().append_pair(key, value);
        }

        if let Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
        } = self.command.clone()
        {
            let mut query_pairs = url.query_pairs_mut();
            delimiter.map(|d| query_pairs.append_pair("delimiter", &d.clone()));
            query_pairs.append_pair("prefix", &prefix);
            query_pairs.append_pair("list-type", "2");
            if let Some(token) = continuation_token {
                query_pairs.append_pair("continuation-token", &token);
            }
        }

        match self.command {
            Command::PutObjectTagging { .. }
            | Command::GetObjectTagging
            | Command::DeleteObjectTagging => {
                url.query_pairs_mut().append_pair("tagging", "");
            }
            _ => {}
        }

        url
    }

    fn content_length(&self) -> usize {
        match self.command {
            Command::PutObject { content, .. } => content.len(),
            Command::PutObjectTagging { tags } => tags.len(),
            _ => 0,
        }
    }

    fn content_type(&self) -> String {
        match self.command {
            Command::PutObject { content_type, .. } => content_type.into(),
            _ => "text/plain".into(),
        }
    }

    fn sha256(&self) -> String {
        match self.command {
            Command::PutObject { content, .. } => {
                let mut sha = Sha256::default();
                sha.input(content);
                hex::encode(sha.result().as_slice())
            }
            Command::PutObjectTagging { tags } => {
                let mut sha = Sha256::default();
                sha.input(tags.as_bytes());
                hex::encode(sha.result().as_slice())
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }

    fn long_date(&self) -> String {
        self.datetime.format(LONG_DATE).to_string()
    }

    fn canonical_request(&self, headers: &HeaderMap) -> String {
        signing::canonical_request(
            self.command.http_verb().as_str(),
            &self.url(),
            headers,
            &self.sha256(),
        )
    }

    fn string_to_sign(&self, request: &str) -> String {
        signing::string_to_sign(&self.datetime, &self.bucket.region(), request)
    }

    fn signing_key(&self) -> Result<Vec<u8>> {
        Ok(signing::signing_key(
            &self.datetime,
            &self.bucket.secret_key(),
            &self.bucket.region(),
            "s3",
        )?)
    }

    fn authorization(&self, headers: &HeaderMap) -> Result<String> {
        let canonical_request = self.canonical_request(headers);
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_varkey(&self.signing_key()?)?;
        hmac.input(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.result().code());
        let signed_header = signing::signed_header_string(headers);
        Ok(signing::authorization_header(
            &self.bucket.access_key(),
            &self.datetime,
            &self.bucket.region(),
            &signed_header,
            &signature,
        ))
    }

    fn headers(&self) -> Result<HeaderMap> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.
        let mut headers = self
            .bucket
            .extra_headers
            .iter()
            .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
            .collect::<Result<HeaderMap, S3Error>>()?;
        // match self.command {
        //     Command::ListBucket { .. } => {
        //         headers.insert(header::HOST, self.bucket.host().parse()?)
        //     }
        //     _ => headers.insert(header::HOST, self.bucket.self_host().parse()?),
        // };
        headers.insert(header::HOST, self.bucket.self_host().parse()?);
        headers.insert(
            header::CONTENT_LENGTH,
            self.content_length().to_string().parse()?,
        );
        headers.insert(header::CONTENT_TYPE, self.content_type().parse()?);
        headers.insert("X-Amz-Content-Sha256", sha256.parse()?);
        headers.insert("X-Amz-Date", self.long_date().parse()?);

        if let Some(token) = self.bucket.credentials().token.as_ref() {
            headers.insert("X-Amz-Security-Token", token.parse()?);
        }

        if let Command::PutObjectTagging { tags } = self.command {
            let digest = md5::compute(tags);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5", hash.parse()?);
        }

        // This must be last, as it signs the other headers
        let authorization = self.authorization(&headers)?;
        headers.insert(header::AUTHORIZATION, authorization.parse()?);

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(header::DATE, self.datetime.to_rfc2822().parse()?);

        Ok(headers)
    }

    pub fn response_data(&self) -> Result<(Vec<u8>, u16)> {
        let response_data = self.response_data_future().map(|result| match result {
            Ok((response_data, status_code)) => Ok((response_data, status_code)),
            Err(e) => Err(e),
        });
        let mut runtime = Runtime::new().unwrap();
        runtime.block_on(response_data)
    }

    pub fn response_data_to_writer<T: Write>(&self, writer: &mut T) -> Result<u16> {
        let status_code_future =
            self.response_data_to_writer_future(writer)
                .map(|result| match result {
                    Ok(status_code) => Ok(status_code),
                    Err(_) => Err(S3Error::from("ReqwestFuture")),
                });
        let mut runtime = Runtime::new().unwrap();
        runtime.block_on(status_code_future)
    }

    pub fn response_future(&self) -> impl Future<Output = Result<Response>> {
        let client = if cfg!(feature = "no-verify-ssl") {
            async::Client::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .expect("Could not build dangereous client!")
        } else {
            async::Client::new()
        };

        // Build headers
        let headers = self.headers().expect("Could not get headers!");

        // Get owned content to pass to reqwest
        let content = if let Command::PutObject { content, .. } = self.command {
            Vec::from(content)
        } else if let Command::PutObjectTagging { tags } = self.command {
            Vec::from(tags)
        } else {
            Vec::new()
        };

        let request = client
            .request(self.command.http_verb(), self.url().as_str())
            .headers(headers.to_owned())
            .body(content.to_owned());

        request.send().map_err(S3Error::from)
    }

    pub fn response_data_future(&self) -> impl Future<Output = Result<(Vec<u8>, u16)>> {
        self.response_future()
            .map(|mut response| Ok((response?.text(), response?.status().as_u16())))
            .and_then(|(body_future, status_code)| {
                body_future
                    .map(move |body| Ok((body?.as_bytes().to_vec(), status_code)))
                    .map_err(S3Error::from)
            })
    }

    pub fn response_data_to_writer_future<'b, T: Write>(
        &self,
        writer: &'b mut T,
    ) -> impl Future<Output = Result<u16>> + 'b {
        let future_response = self.response_data_future();
        future_response.map(move |response| {
            writer
                .write_all(response?.0.as_slice())
                .expect("Could not write to writer");
            Ok(response?.1)
        })
    }
}

#[cfg(test)]
mod tests {
    use bucket::Bucket;
    use command::Command;
    use credentials::Credentials;
    use error::Result;
    use request::Request;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        const ACCESS_KEY: &'static str = "AKIAIOSFODNN7EXAMPLE";
        const SECRET_KEY: &'static str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(ACCESS_KEY.into()), Some(SECRET_KEY.into()), None, None)
    }

    #[test]
    fn url_uses_https_by_default() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = Request::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "https");

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
        let request = Request::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url().scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }
}
