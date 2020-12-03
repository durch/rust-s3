use chrono::{DateTime, Utc};
use hmac::Mac;
use hmac::NewMac;
use maybe_async::maybe_async;
use std::io::Write;
use url::Url;

use crate::bucket::Bucket;
use crate::bucket::Headers;
use crate::command::Command;
use crate::signing;
use anyhow::Result;
use anyhow::anyhow;
use crate::LONG_DATE;

#[maybe_async]
pub trait Request {
    type Response;
    type HeaderMap;

    async fn response(&self) -> Result<Self::Response>;
    async fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16)>;
    async fn response_data_to_writer<'b, T: Write + Send>(&self, writer: &'b mut T) -> Result<u16>;
    async fn response_header(&self) -> Result<(Self::HeaderMap, u16)>;
    fn datetime(&self) -> DateTime<Utc>;
    fn bucket(&self) -> Bucket;
    fn command(&self) -> Command;
    fn path(&self) -> String;

    fn signing_key(&self) -> Result<Vec<u8>> {
        Ok(signing::signing_key(
            &self.datetime(),
            &self
                .bucket()
                .secret_key()
                .expect("Secret key must be provided to sign headers, found None"),
            &self.bucket().region(),
            "s3",
        )?)
    }

    fn request_body(&self) -> Vec<u8> {
        if let Command::PutObject { content, .. } = self.command() {
            Vec::from(content)
        } else if let Command::PutObjectTagging { tags } = self.command() {
            Vec::from(tags)
        } else if let Command::UploadPart { content, .. } = self.command() {
            Vec::from(content)
        } else if let Command::CompleteMultipartUpload { data, .. } = &self.command() {
            let body = data.to_string();
            // assert_eq!(body, "body".to_string());
            body.as_bytes().to_vec()
        } else if let Command::CreateBucket { config } = &self.command() {
            if let Some(payload) = config.location_constraint_payload() {
                Vec::from(payload)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    fn long_date(&self) -> String {
        self.datetime().format(LONG_DATE).to_string()
    }

    fn string_to_sign(&self, request: &str) -> String {
        signing::string_to_sign(&self.datetime(), &self.bucket().region(), request)
    }

    fn host_header(&self) -> String {
        self.bucket().host()
    }

    fn presigned(&self) -> Result<String> {
        let expiry = match self.command() {
            Command::PresignGet { expiry_secs } => expiry_secs,
            Command::PresignPut { expiry_secs, .. } => expiry_secs,
            _ => unreachable!(),
        };

        if let Command::PresignPut { custom_headers, .. } = self.command() {
            if let Some(custom_headers) = custom_headers {
                let authorization = self.presigned_authorization(Some(&custom_headers))?;
                return Ok(format!(
                    "{}&X-Amz-Signature={}",
                    self.presigned_url_no_sig(expiry, Some(&custom_headers))?,
                    authorization
                ));
            }
        }

        Ok(format!(
            "{}&X-Amz-Signature={}",
            self.presigned_url_no_sig(expiry, None)?,
            self.presigned_authorization(None)?
        ))
    }

    fn presigned_authorization(&self, custom_headers: Option<&Headers>) -> Result<String> {
        let mut headers = Headers::new();
        let host_header = self.host_header();
        headers.insert("Host".to_string(), host_header);
        if let Some(custom_headers) = custom_headers {
            for (k, v) in custom_headers.iter() {
                headers.insert(k.clone(), v.clone());
            }
        }
        let canonical_request = self.presigned_canonical_request(&headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_varkey(&self.signing_key()?).map_err(|e| anyhow!{"{}",e})?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        // let signed_header = signing::signed_header_string(&headers);
        Ok(signature)
    }

    fn presigned_canonical_request(&self, headers: &Headers) -> Result<String> {
        let expiry = match self.command() {
            Command::PresignGet { expiry_secs } => expiry_secs,
            Command::PresignPut { expiry_secs, .. } => expiry_secs,
            _ => unreachable!(),
        };

        if let Command::PresignPut { custom_headers, .. } = self.command() {
            if let Some(custom_headers) = custom_headers {
                return Ok(signing::canonical_request(
                    &self.command().http_verb().to_string(),
                    &self.presigned_url_no_sig(expiry, Some(&custom_headers))?,
                    headers,
                    "UNSIGNED-PAYLOAD",
                ));
            }
        }

        Ok(signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.presigned_url_no_sig(expiry, None)?,
            headers,
            "UNSIGNED-PAYLOAD",
        ))
    }

    fn presigned_url_no_sig(&self, expiry: u32, custom_headers: Option<&Headers>) -> Result<Url> {
        let bucket = self.bucket();
        let token = if let Some(security_token) = bucket.security_token() {
            Some(security_token)
        } else if let Some(session_token) = bucket.session_token() {
            Some(session_token)
        } else {
            None
        };
        let url = Url::parse(&format!(
            "{}{}",
            self.url(true),
            &signing::authorization_query_params_no_sig(
                &self.bucket().access_key().unwrap(),
                &self.datetime(),
                &self.bucket().region(),
                expiry,
                custom_headers,
                token
            )?
        ))?;

        Ok(url)
    }

    fn url(&self, encode_path: bool) -> Url {
        let mut url_str = self.bucket().url();

        if let Command::CreateBucket { .. } = self.command() {
            return Url::parse(&url_str).unwrap();
        }

        let path = if self.path().starts_with('/') {
            self.path()[1..].to_string()
        } else {
            self.path()[..].to_string()
        };

        url_str.push_str("/");

        if encode_path {
            url_str.push_str(&signing::uri_encode(&path, true));
        } else {
            url_str.push_str(&path);
        }

        // Since every part of this URL is either pre-encoded or statically
        // generated, there's really no way this should fail.
        let mut url = Url::parse(&url_str).expect("static URL parsing");

        for (key, value) in &self.bucket().extra_query {
            url.query_pairs_mut().append_pair(key, value);
        }

        if let Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
            start_after,
            max_keys,
        } = self.command().clone()
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

        match self.command() {
            Command::PutObjectTagging { .. }
            | Command::GetObjectTagging
            | Command::DeleteObjectTagging => {
                url.query_pairs_mut().append_pair("tagging", "");
            }
            _ => {}
        }

        url
    }

    fn canonical_request(&self, headers: &Headers) -> String {
        signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.url(false),
            headers,
            &self.command().sha256(),
        )
    }

    fn authorization(&self, headers: &Headers) -> Result<String> {
        let canonical_request = self.canonical_request(headers);
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_varkey(&self.signing_key()?).map_err(|e| anyhow!{"{}",e})?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let signed_header = signing::signed_header_string(headers);
        Ok(signing::authorization_header(
            &self.bucket().access_key().unwrap(),
            &self.datetime(),
            &self.bucket().region(),
            &signed_header,
            &signature,
        ))
    }

    fn headers(&self) -> Result<Headers> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.command().sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.

        let mut headers = Headers::new();

        for (k, v) in self.bucket().extra_headers.iter() {
            headers.insert(k.clone(), v.clone());
        }

        let host_header = self.host_header();

        headers.insert("Host".to_string(), host_header);

        match self.command() {
            Command::ListBucket { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}
            _ => {
                headers.insert(
                    "Content-Length".to_string(),
                    self.command().content_length().to_string(),
                );
                headers.insert("Content-Type".to_string(), self.command().content_type());
            }
        }
        headers.insert("X-Amz-Content-Sha256".to_string(), sha256);
        headers.insert("X-Amz-Date".to_string(), self.long_date());

        if let Some(session_token) = self.bucket().session_token() {
            headers.insert(
                "X-Amz-Security-Token".to_string(),
                session_token.to_string(),
            );
        } else if let Some(security_token) = self.bucket().security_token() {
            headers.insert(
                "X-Amz-Security-Token".to_string(),
                security_token.to_string(),
            );
        }

        if let Command::PutObjectTagging { tags } = self.command() {
            let digest = md5::compute(tags);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5".to_string(), hash);
        } else if let Command::PutObject { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5".to_string(), hash);
        } else if let Command::UploadPart { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert("Content-MD5".to_string(), hash);
        } else if let Command::GetObject {} = self.command() {
            headers.insert("Accept".to_string(), "application/octet-stream".to_string());
        // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        } else if let Command::GetObjectRange { start, end } = self.command() {
            headers.insert("Accept".to_string(), "application/octet-stream".to_string());

            let mut range = format!("bytes={}-", start);

            if let Some(end) = end {
                range.push_str(&end.to_string());
            }

            headers.insert("Range".to_string(), range);
        } else if let Command::CreateBucket { ref config } = self.command() {
            config.add_headers(&mut headers)?;
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket().secret_key().is_some() {
            let authorization = self.authorization(&headers)?;
            headers.insert("Authorization".to_string(), authorization);
        }

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert("Date".to_string(), self.datetime().to_rfc2822());

        Ok(headers)
    }
}
