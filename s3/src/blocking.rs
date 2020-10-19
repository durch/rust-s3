extern crate base64;
extern crate md5;

use std::collections::HashMap;
use std::io::Write;

use super::bucket::Bucket;
use super::command::Command;
use chrono::{DateTime, Utc};
use hmac::{Mac, NewMac};
use attohttpc::header::{self, HeaderMap, HeaderName, HeaderValue};
use attohttpc::Response;
use sha2::{Digest, Sha256};
use url::Url;

use crate::signing;

use crate::EMPTY_PAYLOAD_SHA;
use crate::LONG_DATE;
use crate::{Result, S3Error};
use crate::command::HttpMethod;

/// Collection of HTTP headers sent to S3 service, in key/value format.
pub type Headers = HashMap<String, String>;

/// Collection of HTTP query parameters sent to S3 service, in key/value
/// format.
pub type Query = HashMap<String, String>;

// static CLIENT: Lazy<Client> = Lazy::new(|| {
//     if cfg!(feature = "no-verify-ssl") {
//         Client::builder()
//             .danger_accept_invalid_certs(true)
//             .danger_accept_invalid_hostnames(true)
//             .build()
//             .expect("Could not build dangerous client!")
//     } else {
//         Client::new()
//     }
// });

impl std::convert::From<attohttpc::Error> for S3Error {
    fn from(e: attohttpc::Error) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None
        }
    }
}

impl std::convert::From<http::header::InvalidHeaderValue> for S3Error {
    fn from(e: http::header::InvalidHeaderValue) -> S3Error {
        S3Error {
            description: Some(format!("{}", e)),
            data: None,
            source: None
        }
    }
}

// Temporary structure for making a request
pub struct Request<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: DateTime<Utc>,
    pub sync: bool,
}

impl<'a> Request<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Request<'b> {
        Request {
            bucket,
            path,
            command,
            datetime: Utc::now(),
            sync: false,
        }
    }

    pub fn presigned(&self) -> Result<String> {
        let expiry = match self.command {
            Command::PresignGet { expiry_secs } => expiry_secs,
            Command::PresignPut { expiry_secs, .. } => expiry_secs,
            _ => unreachable!(),
        };
        let custom_headers = match &self.command {
            Command::PresignPut {custom_headers, .. } => {
                if let Some(custom_headers) = custom_headers {
                    Some(custom_headers.clone())
                } else {
                    None
                }
            },
            _ => None  
        };
        let authorization = self.presigned_authorization(custom_headers.clone())?;
        Ok(format!(
            "{}&X-Amz-Signature={}",
            self.presigned_url_no_sig(expiry, custom_headers)?,
            authorization
        ))
    }

    fn host_header(&self) -> Result<HeaderValue> {
        let host = self.bucket.host();
        HeaderValue::from_str(&host).map_err(|_e| {
            S3Error::from(format!("Could not parse HOST header value {}", host).as_ref())
        })
    }

    fn url(&self, encode_path: bool) -> Url {
        let mut url_str = self.bucket.url();

        let path = if self.path.starts_with('/') {
            &self.path[1..]
        } else {
            &self.path[..]
        };


        url_str.push_str("/");

        if encode_path {
            url_str.push_str(&signing::uri_encode(path, true));
        } else {
            url_str.push_str(path);
        }
    

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
            start_after,
            max_keys,
        } = self.command.clone()
        {
            let mut query_pairs = url.query_pairs_mut();
            delimiter.map(|d| query_pairs.append_pair("delimiter", &d));
            query_pairs.append_pair("prefix", &prefix);
            query_pairs.append_pair("list-type", "2");
            if let Some(token) = continuation_token {
                query_pairs.append_pair("continuation-token", &token);
            }
            if let Some(start_after) = start_after {
                query_pairs.append_pair("start-after", &start_after);
            }
            if let Some(max_keys) = max_keys {
                query_pairs.append_pair("max-keys", &max_keys.to_string());
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
        match &self.command {
            Command::PutObject { content, .. } => content.len(),
            Command::PutObjectTagging { tags } => tags.len(),
            Command::UploadPart { content, .. } => content.len(),
            Command::CompleteMultipartUpload { data, .. } => data.len(),
            _ => 0,
        }
    }

    fn content_type(&self) -> String {
        match self.command {
            Command::PutObject { content_type, .. } => content_type.into(),
            Command::CompleteMultipartUpload { .. } => "application/xml".into(),
            _ => "text/plain".into(),
        }
    }

    fn sha256(&self) -> String {
        match &self.command {
            Command::PutObject { content, .. } => {
                let mut sha = Sha256::default();
                sha.update(content);
                hex::encode(sha.finalize().as_slice())
            }
            Command::PutObjectTagging { tags } => {
                let mut sha = Sha256::default();
                sha.update(tags.as_bytes());
                hex::encode(sha.finalize().as_slice())
            }
            Command::CompleteMultipartUpload { data, .. } => {
                let mut sha = Sha256::default();
                sha.update(data.to_string().as_bytes());
                hex::encode(sha.finalize().as_slice())
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }

    fn long_date(&self) -> String {
        self.datetime.format(LONG_DATE).to_string()
    }

    fn canonical_request(&self, headers: &HeaderMap) -> String {
        signing::canonical_request(
            &self.command.http_verb().to_string(),
            &self.url(false),
            headers,
            &self.sha256(),
        )
    }

    fn presigned_url_no_sig(&self, expiry: u32, custom_headers: Option<HeaderMap>) -> Result<Url> {
        let token = if let Some(security_token) = self.bucket.security_token() {
            Some(security_token)
        } else if let Some(session_token) = self.bucket.session_token() {
            Some(session_token)
        } else {
            None
        };
        let url = Url::parse(&format!(
            "{}{}",
            self.url(true),
            &signing::authorization_query_params_no_sig(
                &self.bucket.access_key().unwrap(),
                &self.datetime,
                &self.bucket.region(),
                expiry,
                custom_headers,
                token
            )?
        ))?;

        Ok(url)
    }

    fn presigned_canonical_request(&self, headers: &HeaderMap) -> Result<String> {
        let expiry = match self.command {
            Command::PresignGet { expiry_secs } => expiry_secs,
            Command::PresignPut { expiry_secs, .. } => expiry_secs,
            _ => unreachable!(),
        };

        let custom_headers = match &self.command {
            Command::PresignPut {custom_headers, ..} => {
                if let Some(custom_headers) = custom_headers {
                    Some(custom_headers.clone())
                } else {
                    None
                }
            },
            _ => None
        };

        let canonical_request = signing::canonical_request(
            &self.command.http_verb().to_string(),
            &self.presigned_url_no_sig(expiry, custom_headers)?,
            headers,
            "UNSIGNED-PAYLOAD",
        );
        Ok(canonical_request)
    }

    fn string_to_sign(&self, request: &str) -> String {
        signing::string_to_sign(&self.datetime, &self.bucket.region(), request)
    }

    fn signing_key(&self) -> Result<Vec<u8>> {
        Ok(signing::signing_key(
            &self.datetime,
            &self
                .bucket
                .secret_key()
                .expect("Secret key must be provided to sign headers, found None"),
            &self.bucket.region(),
            "s3",
        )?)
    }

    fn presigned_authorization(&self, custom_headers: Option<HeaderMap>) -> Result<String> {
        let mut headers = HeaderMap::new();
        let host_header = self.host_header()?;
        headers.insert(header::HOST, host_header);
        if let Some(custom_headers) = custom_headers {
            for (k, v) in custom_headers.into_iter() {
                if let Some(k) = k {
                    headers.insert(k, v);
                }
            }
        }
        let canonical_request = self.presigned_canonical_request(&headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_varkey(&self.signing_key()?)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        // let signed_header = signing::signed_header_string(&headers);
        Ok(signature)
    }

    fn authorization(&self, headers: &HeaderMap) -> Result<String> {
        let canonical_request = self.canonical_request(headers);
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_varkey(&self.signing_key()?)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let signed_header = signing::signed_header_string(headers);
        Ok(signing::authorization_header(
            &self.bucket.access_key().unwrap(),
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

        let host_header = self.host_header()?;

        headers.insert(header::HOST, host_header);

        match self.command {
            Command::ListBucket { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}
            _ => {
                headers.insert(
                    header::CONTENT_LENGTH,
                    match self.content_length().to_string().parse() {
                        Ok(content_length) => content_length,
                        Err(_) => {
                            return Err(S3Error::from(
                                format!(
                                    "Could not parse CONTENT_LENGTH header value {}",
                                    self.content_length()
                                )
                                .as_ref(),
                            ))
                        }
                    },
                );
                headers.insert(
                    header::CONTENT_TYPE,
                    match self.content_type().parse() {
                        Ok(content_type) => content_type,
                        Err(_) => {
                            return Err(S3Error::from(
                                format!(
                                    "Could not parse CONTENT_TYPE header value {}",
                                    self.content_type()
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
            match self.long_date().parse() {
                Ok(value) => value,
                Err(_) => {
                    return Err(S3Error::from(
                        format!(
                            "Could not parse X-Amz-Date header value {}",
                            self.long_date()
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
        } else if let Command::UploadPart { content, .. } = self.command {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5", hash.parse()?);
        } else if let Command::GetObject {} = self.command {
            headers.insert(
                header::ACCEPT,
                HeaderValue::from_str("application/octet-stream")?,
            );
            // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket.secret_key().is_some() {
            let authorization = self.authorization(&headers)?;
            headers.insert(
                header::AUTHORIZATION,
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
            header::DATE,
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
        } else if let Command::UploadPart { content, .. } = self.command {
            Vec::from(content)
        } else if let Command::CompleteMultipartUpload { data, .. } = &self.command {
            let body = data.to_string();
            // assert_eq!(body, "body".to_string());
            body.as_bytes().to_vec()
        } else {
            Vec::new()
        };

        // let client = if cfg!(feature = "no-verify-ssl") {
        //     let client = Client::builder().danger_accept_invalid_certs(true);

        //         cfg_if::cfg_if! {
        //             if #[cfg(feature = "native-tls")]
        //             {
        //                 let client = client.danger_accept_invalid_hostnames(true);
        //             }

        //         }

        //         client.build()
        //         .expect("Could not build dangerous client!")
        // } else {
        //     Client::new()
        // };

        // let request = client
        //     .request(self.command.http_verb(), self.url(false).as_str())
        //     .headers(headers.to_owned())
        //     .body(content.to_owned());

        let mut session = attohttpc::Session::new();

        for (name, value) in headers {
            if let Some(name) = name {
                session.header(name, value);
            }
            
        }

        let request = match self.command.http_verb() {
            HttpMethod::Get => session.get(self.url(false)),
            HttpMethod::Delete => session.delete(self.url(false)),
            HttpMethod::Put => session.put(self.url(false)),
            HttpMethod::Post => session.post(self.url(false))
        };

        let response = request.bytes(&content).send()?;

        // let response = request.send()?;

        if cfg!(feature = "fail-on-err") && response.status().as_u16() >= 400 {
            return Err(S3Error::from(
                format!(
                    "Request failed with code {}\n{}",
                    response.status().as_u16(),
                    response.text()?
                )
                .as_str(),
            ));
        }

        Ok(response)
    }

    pub fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16)> {
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

    pub fn response_data_to_writer<'b, T: Write>(
        &self,
        writer: &'b mut T,
    ) -> Result<u16> {
        let response = self.response()?;

        let status_code = response.status();
        let stream = response.bytes()?;

        writer.write_all(&stream)?;

        Ok(status_code.as_u16())
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::blocking::Request;
    use crate::Result;
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
        let request = Request::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "https");

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
        let request = Request::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "https");

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

        assert_eq!(request.url(false).scheme(), "http");

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
        let request = Request::new(&bucket, path, Command::GetObject);

        assert_eq!(request.url(false).scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }
}
