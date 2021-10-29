use chrono::{DateTime, Utc};
use hmac::Mac;
use hmac::NewMac;
use url::Url;

use crate::bucket::Bucket;
use crate::command::Command;
use crate::signing;
use crate::LONG_DATE;
use anyhow::anyhow;
use anyhow::Result;
use http::header::{
    HeaderName, ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST, RANGE,
};
use http::HeaderMap;

#[maybe_async::maybe_async]
pub trait Request {
    type Response;
    type HeaderMap;

    async fn response(&self) -> Result<Self::Response>;
    async fn response_data(&self, etag: bool) -> Result<(Vec<u8>, u16)>;
    #[cfg(feature = "with-tokio")]
    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<u16>;
    #[cfg(feature = "with-async-std")]
    async fn response_data_to_writer<T: futures::io::AsyncWrite + Send + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<u16>;
    #[cfg(feature = "sync")]
    fn response_data_to_writer<T: std::io::Write + Send>(&self, writer: &mut T) -> Result<u16>;
    async fn response_header(&self) -> Result<(Self::HeaderMap, u16)>;
    fn datetime(&self) -> DateTime<Utc>;
    fn bucket(&self) -> Bucket;
    fn command(&self) -> Command;
    fn path(&self) -> String;

    fn signing_key(&self) -> Result<Vec<u8>> {
        signing::signing_key(
            &self.datetime(),
            &self
                .bucket()
                .secret_key()
                .expect("Secret key must be provided to sign headers, found None"),
            &self.bucket().region(),
            "s3",
        )
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
            Command::PresignDelete { expiry_secs } => expiry_secs,
            _ => unreachable!(),
        };

        #[allow(clippy::collapsible_match)]
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

    fn presigned_authorization(&self, custom_headers: Option<&HeaderMap>) -> Result<String> {
        let mut headers = HeaderMap::new();
        let host_header = self.host_header();
        headers.insert(HOST, host_header.parse().unwrap());
        if let Some(custom_headers) = custom_headers {
            for (k, v) in custom_headers.iter() {
                headers.insert(k.clone(), v.clone());
            }
        }
        let canonical_request = self.presigned_canonical_request(&headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_from_slice(&self.signing_key()?)
            .map_err(|e| anyhow! {"{}",e})?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        // let signed_header = signing::signed_header_string(&headers);
        Ok(signature)
    }

    fn presigned_canonical_request(&self, headers: &HeaderMap) -> Result<String> {
        let expiry = match self.command() {
            Command::PresignGet { expiry_secs } => expiry_secs,
            Command::PresignPut { expiry_secs, .. } => expiry_secs,
            Command::PresignDelete { expiry_secs } => expiry_secs,
            _ => unreachable!(),
        };

        #[allow(clippy::collapsible_match)]
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

    fn presigned_url_no_sig(&self, expiry: u32, custom_headers: Option<&HeaderMap>) -> Result<Url> {
        let bucket = self.bucket();
        let token = if let Some(security_token) = bucket.security_token() {
            Some(security_token)
        } else {
            bucket.session_token()
        };
        let url = Url::parse(&format!(
            "{}{}",
            self.url(),
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

    fn url(&self) -> Url {
        let mut url_str = self.bucket().url();

        if let Command::CreateBucket { .. } = self.command() {
            return Url::parse(&url_str).unwrap();
        }

        let path = if self.path().starts_with('/') {
            self.path()[1..].to_string()
        } else {
            self.path()[..].to_string()
        };

        url_str.push('/');

        url_str.push_str(&signing::uri_encode(&path, true));

        // Append to url_path
        #[allow(clippy::collapsible_match)]
        match self.command() {
            Command::InitiateMultipartUpload | Command::ListMultipartUploads { .. } => {
                url_str.push_str("?uploads")
            }
            Command::AbortMultipartUpload { upload_id } => {
                url_str.push_str(&format!("?uploadId={}", upload_id))
            }
            Command::CompleteMultipartUpload { upload_id, .. } => {
                url_str.push_str(&format!("?uploadId={}", upload_id))
            }
            Command::GetObjectTorrent => url_str.push_str("?torrent"),
            Command::PutObject { multipart, .. } => {
                if let Some(multipart) = multipart {
                    url_str.push_str(&multipart.query_string())
                }
            }
            _ => {}
        }

        // Since every part of this URL is either pre-encoded or statically
        // generated, there's really no way this should fail.
        let mut url = Url::parse(&url_str).expect("static URL parsing");

        for (key, value) in &self.bucket().extra_query {
            url.query_pairs_mut().append_pair(key, value);
        }

        // println!("{}", url_str);

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
            Command::ListMultipartUploads {
                prefix,
                delimiter,
                key_marker,
                max_uploads,
            } => {
                let mut query_pairs = url.query_pairs_mut();
                delimiter.map(|d| query_pairs.append_pair("delimiter", d));
                if let Some(prefix) = prefix {
                    query_pairs.append_pair("prefix", prefix);
                }
                if let Some(key_marker) = key_marker {
                    query_pairs.append_pair("key-marker", &key_marker);
                }
                if let Some(max_uploads) = max_uploads {
                    query_pairs.append_pair("max-uploads", max_uploads.to_string().as_str());
                }
            }
            Command::PutObjectTagging { .. }
            | Command::GetObjectTagging
            | Command::DeleteObjectTagging => {
                url.query_pairs_mut().append_pair("tagging", "");
            }
            _ => {}
        }

        url
    }

    fn canonical_request(&self, headers: &HeaderMap) -> String {
        signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.url(),
            headers,
            &self.command().sha256(),
        )
    }

    fn authorization(&self, headers: &HeaderMap) -> Result<String> {
        let canonical_request = self.canonical_request(headers);
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = signing::HmacSha256::new_from_slice(&self.signing_key()?)
            .map_err(|e| anyhow! {"{}",e})?;
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

    fn headers(&self) -> Result<HeaderMap> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.command().sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.

        let mut headers = HeaderMap::new();

        for (k, v) in self.bucket().extra_headers.iter() {
            headers.insert(k.clone(), v.clone());
        }

        let host_header = self.host_header();

        headers.insert(HOST, host_header.parse().unwrap());

        match self.command() {
            Command::CopyObject { from } => {
                headers.insert(
                    HeaderName::from_static("x-amz-copy-source"),
                    from.parse().unwrap(),
                );
            }
            Command::ListBucket { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}
            _ => {
                headers.insert(
                    CONTENT_LENGTH,
                    self.command().content_length().to_string().parse().unwrap(),
                );
                headers.insert(CONTENT_TYPE, self.command().content_type().parse().unwrap());
            }
        }
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            sha256.parse().unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            self.long_date().parse().unwrap(),
        );

        if let Some(session_token) = self.bucket().session_token() {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                session_token.to_string().parse().unwrap(),
            );
        } else if let Some(security_token) = self.bucket().security_token() {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                security_token.to_string().parse().unwrap(),
            );
        }

        if let Command::PutObjectTagging { tags } = self.command() {
            let digest = md5::compute(tags);
            let hash = base64::encode(digest.as_ref());
            headers.insert(
                HeaderName::from_static("content-md5"),
                hash.parse().unwrap(),
            );
        } else if let Command::PutObject { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert(
                HeaderName::from_static("content-md5"),
                hash.parse().unwrap(),
            );
        } else if let Command::UploadPart { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert(
                HeaderName::from_static("content-md5"),
                hash.parse().unwrap(),
            );
        } else if let Command::GetObject {} = self.command() {
            headers.insert(
                ACCEPT,
                "application/octet-stream".to_string().parse().unwrap(),
            );
        // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        } else if let Command::GetObjectRange { start, end } = self.command() {
            headers.insert(
                ACCEPT,
                "application/octet-stream".to_string().parse().unwrap(),
            );

            let mut range = format!("bytes={}-", start);

            if let Some(end) = end {
                range.push_str(&end.to_string());
            }

            headers.insert(RANGE, range.parse().unwrap());
        } else if let Command::CreateBucket { ref config } = self.command() {
            config.add_headers(&mut headers)?;
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket().secret_key().is_some() {
            let authorization = self.authorization(&headers)?;
            headers.insert(AUTHORIZATION, authorization.parse().unwrap());
        }

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(DATE, self.datetime().to_rfc2822().parse().unwrap());

        Ok(headers)
    }
}
