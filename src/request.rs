use std::collections::HashMap;
use std::io::{Read, Cursor};

use bucket::Bucket;
use chrono::{DateTime, Utc};
use command::Command;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use curl::easy::{Easy, List, ReadError};
use hex::ToHex;
use url::Url;
use serde_xml;

use serde_types::AwsError;
use signing;
use error::{S3Result, ErrorKind};

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
}

impl<'a> Request<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Request<'b> {
        Request {
            bucket: bucket,
            path: path,
            command: command,
            datetime: Utc::now(),
        }
    }

    fn url(&self) -> Url {
        let mut url_str = String::from("https://");
        url_str.push_str(self.bucket.host());
        url_str.push_str("/");
        url_str.push_str(self.bucket.name());
        if !self.path.starts_with('/') {
            url_str.push_str("/");
        }
        url_str.push_str(&signing::uri_encode(self.path, false));

        // Since every part of this URL is either pre-encoded or statically
        // generated, there's really no way this should fail.
        let mut url = Url::parse(&url_str).expect("static URL parsing");

        for (key, value) in self.bucket.extra_query.iter() {
            url.query_pairs_mut().append_pair(key, value);
        }

        if let Command::List { prefix, delimiter, continuation_token } = self.command {
            let mut query_pairs = url.query_pairs_mut();
            delimiter.map(|d| query_pairs.append_pair("delimiter", d));
            query_pairs.append_pair("prefix", prefix);
            query_pairs.append_pair("list-type", "2");
            if let Some(token) = continuation_token {
                query_pairs.append_pair("continuation-token", token);
            }
        }

        url
    }

    fn content_length(&self) -> usize {
        match self.command {
            Command::Put { content, .. } => content.len(),
            _ => 0,
        }
    }

    fn content_type(&self) -> String {
        match self.command {
            Command::Put { content_type, .. } => content_type.into(),
            _ => "text/plain".into(),
        }
    }

    fn sha256(&self) -> String {
        match self.command {
            Command::Put { content, .. } => {
                let mut sha = Sha256::default();
                sha.input(content);
                sha.result().as_slice().to_hex()
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }

    fn long_date(&self) -> String {
        self.datetime.format(LONG_DATE).to_string()
    }

    fn canonical_request(&self, headers: &Headers) -> String {
        signing::canonical_request(self.command.http_verb(),
                                   &self.url(),
                                   headers,
                                   &self.sha256())
    }

    fn string_to_sign(&self, request: &str) -> String {
        signing::string_to_sign(&self.datetime, self.bucket.region(), &request)
    }

    fn signing_key(&self) -> Vec<u8> {
        signing::signing_key(&self.datetime,
                             self.bucket.secret_key(),
                             self.bucket.region(),
                             "s3")
    }

    fn authorization(&self, headers: &Headers) -> String {
        let canonical_request = self.canonical_request(headers);
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = Hmac::<Sha256>::new(&self.signing_key());
        hmac.input(string_to_sign.as_bytes());
        let signature = hmac.result().code().to_hex();
        let signed_header = signing::signed_header_string(headers);
        signing::authorization_header(self.bucket.access_key(),
                                      &self.datetime,
                                      self.bucket.region(),
                                      &signed_header,
                                      &signature)
    }

    fn headers(&self) -> S3Result<Headers> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.
        let mut headers: Headers = self.bucket.extra_headers.clone();
        headers.insert("Host".into(), self.bucket.host().into());
        headers.insert("Content-Length".into(), self.content_length().to_string());
        headers.insert("Content-Type".into(), self.content_type());
        headers.insert("X-Amz-Content-Sha256".into(), sha256.clone());
        headers.insert("X-Amz-Date".into(), self.long_date());

        self.bucket.credentials().token.as_ref().map(|token| {
            headers.insert("X-Amz-Security-Token".into(), token.clone());
        });

        // This must be last, as it signs the other headers
        let authorization = self.authorization(&headers);
        headers.insert("Authorization".into(), authorization);

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert("Date".into(), self.datetime.to_rfc2822());

        Ok(headers)
    }

    fn load_content(&self, handle: &mut Easy) -> S3Result<Cursor<&[u8]>> {
        if let Command::Put { content, .. } = self.command {
            try!(handle.put(true));
            try!(handle.post_field_size(content.len() as u64));
            Ok(Cursor::new(content))
        } else {
            Ok(Cursor::new(&[] as &[u8]))
        }
    }

    pub fn execute(&self) -> S3Result<(Vec<u8>, u32)> {
        let mut handle = Easy::new();
        handle.url(self.url().as_str())?;

        // Special handling to load PUT content
        let mut content_cursor = try!(self.load_content(&mut handle));

        // Set GET, PUT, etc
        handle.custom_request(self.command.http_verb())?;

        // Build and set a Curl List of headers
        let mut list = List::new();
        for (key, value) in try!(self.headers()).iter() {
            let header = format!("{}: {}", key, value);
            list.append(&header)?;
        }
        handle.http_headers(list)?;

        // Run the transfer
        let mut dst = Vec::new();
        {
            let mut transfer = handle.transfer();

            transfer.read_function(|buf| content_cursor.read(buf).or(Err(ReadError::Abort)))?;

            transfer.write_function(|data| {
                dst.extend_from_slice(data);
                Ok(data.len())
            })?;

            transfer.perform()?;
        }
        let resp_code = handle.response_code()?;
        if resp_code < 300 {
            Ok((dst, resp_code))
        } else {
            let deserialized: AwsError = serde_xml::deserialize(dst.as_slice())?;
            let err = ErrorKind::AwsError {
                info: deserialized,
                status: resp_code,
                body: String::from_utf8_lossy(dst.as_slice()).into_owned()
            };
            Err(err.into())
        }
    }
}
