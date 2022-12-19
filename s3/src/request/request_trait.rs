use hmac::Mac;
use std::collections::HashMap;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use std::pin::Pin;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;
use url::Url;

use crate::bucket::Bucket;
use crate::command::Command;
use crate::error::S3Error;
use crate::signing;
use crate::LONG_DATETIME;
use bytes::Bytes;
use http::header::{
    HeaderName, ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST, RANGE,
};
use http::HeaderMap;
use std::fmt::Write as _;

#[cfg(feature = "with-async-std")]
use futures_util::Stream;

#[cfg(feature = "with-tokio")]
use tokio_stream::Stream;

#[derive(Debug)]

pub struct ResponseData {
    bytes: Bytes,
    status_code: u16,
    headers: HashMap<String, String>,
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub struct ResponseDataStream {
    pub bytes: Pin<Box<dyn Stream<Item = Bytes>>>,
    pub status_code: u16,
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
impl ResponseDataStream {
    pub fn bytes(&mut self) -> &mut Pin<Box<dyn Stream<Item = Bytes>>> {
        &mut self.bytes
    }
}

impl From<ResponseData> for Vec<u8> {
    fn from(data: ResponseData) -> Vec<u8> {
        data.to_vec()
    }
}

impl ResponseData {
    pub fn new(bytes: Bytes, status_code: u16, headers: HashMap<String, String>) -> ResponseData {
        ResponseData {
            bytes,
            status_code,
            headers,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice())
    }

    pub fn to_string(&self) -> Result<String, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice()).map(|s| s.to_string())
    }

    pub fn headers(&self) -> HashMap<String, String> {
        self.headers.clone()
    }
}

use std::fmt;

impl fmt::Display for ResponseData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Status code: {}\n Data: {}",
            self.status_code(),
            self.to_string()
                .unwrap_or_else(|_| "Data could not be cast to UTF string".to_string())
        )
    }
}

#[maybe_async::maybe_async]
pub trait Request {
    type Response;
    type HeaderMap;

    async fn response(&self) -> Result<Self::Response, S3Error>;
    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error>;
    #[cfg(feature = "with-tokio")]
    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(feature = "with-async-std")]
    async fn response_data_to_writer<T: futures_io::AsyncWrite + Send + Unpin>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(feature = "sync")]
    fn response_data_to_writer<T: std::io::Write + Send>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(any(feature = "with-async-std", feature = "with-tokio"))]
    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error>;
    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error>;
    fn datetime(&self) -> OffsetDateTime;
    fn bucket(&self) -> Bucket;
    fn command(&self) -> Command;
    fn path(&self) -> String;

    fn signing_key(&self) -> Result<Vec<u8>, S3Error> {
        signing::signing_key(
            &self.datetime(),
            &self
                .bucket()
                .secret_key()?
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
            println!("CompleteMultipartUpload: {}", body);
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

    fn long_date(&self) -> Result<String, S3Error> {
        Ok(self.datetime().format(LONG_DATETIME)?)
    }

    fn string_to_sign(&self, request: &str) -> Result<String, S3Error> {
        match self.command() {
            Command::PresignPost { post_policy, .. } => Ok(post_policy),
            _ => Ok(signing::string_to_sign(
                &self.datetime(),
                &self.bucket().region(),
                request,
            )?),
        }
    }

    fn host_header(&self) -> String {
        self.bucket().host()
    }

    fn presigned(&self) -> Result<String, S3Error> {
        let (expiry, custom_headers, custom_queries) = match self.command() {
            Command::PresignGet {
                expiry_secs,
                custom_queries,
            } => (expiry_secs, None, custom_queries),
            Command::PresignPut {
                expiry_secs,
                custom_headers,
            } => (expiry_secs, custom_headers, None),
            Command::PresignDelete { expiry_secs } => (expiry_secs, None, None),
            _ => unreachable!(),
        };

        Ok(format!(
            "{}&X-Amz-Signature={}",
            self.presigned_url_no_sig(expiry, custom_headers.as_ref(), custom_queries.as_ref())?,
            self.presigned_authorization(custom_headers.as_ref())?
        ))
    }

    fn presigned_authorization(
        &self,
        custom_headers: Option<&HeaderMap>,
    ) -> Result<String, S3Error> {
        let mut headers = HeaderMap::new();
        let host_header = self.host_header();
        headers.insert(HOST, host_header.parse()?);
        if let Some(custom_headers) = custom_headers {
            for (k, v) in custom_headers.iter() {
                headers.insert(k.clone(), v.clone());
            }
        }
        let canonical_request = self.presigned_canonical_request(&headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request)?;
        let mut hmac = signing::HmacSha256::new_from_slice(&self.signing_key()?)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        // let signed_header = signing::signed_header_string(&headers);
        Ok(signature)
    }

    fn presigned_canonical_request(&self, headers: &HeaderMap) -> Result<String, S3Error> {
        let (expiry, custom_headers, custom_queries) = match self.command() {
            Command::PresignGet {
                expiry_secs,
                custom_queries,
            } => (expiry_secs, None, custom_queries),
            Command::PresignPut {
                expiry_secs,
                custom_headers,
            } => (expiry_secs, custom_headers, None),
            Command::PresignDelete { expiry_secs } => (expiry_secs, None, None),
            _ => unreachable!(),
        };

        signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.presigned_url_no_sig(expiry, custom_headers.as_ref(), custom_queries.as_ref())?,
            headers,
            "UNSIGNED-PAYLOAD",
        )
    }

    fn presigned_url_no_sig(
        &self,
        expiry: u32,
        custom_headers: Option<&HeaderMap>,
        custom_queries: Option<&HashMap<String, String>>,
    ) -> Result<Url, S3Error> {
        let bucket = self.bucket();
        let token = if let Some(security_token) = bucket.security_token()? {
            Some(security_token)
        } else {
            bucket.session_token()?
        };
        let url = Url::parse(&format!(
            "{}{}{}",
            self.url()?,
            &signing::authorization_query_params_no_sig(
                &self.bucket().access_key()?.unwrap_or_default(),
                &self.datetime(),
                &self.bucket().region(),
                expiry,
                custom_headers,
                token.as_ref()
            )?,
            &signing::flatten_queries(custom_queries)?,
        ))?;

        Ok(url)
    }

    fn url(&self) -> Result<Url, S3Error> {
        let mut url_str = self.bucket().url();

        if let Command::CreateBucket { .. } = self.command() {
            return Ok(Url::parse(&url_str)?);
        }

        let path = if self.path().starts_with('/') {
            self.path()[1..].to_string()
        } else {
            self.path()[..].to_string()
        };

        url_str.push('/');
        url_str.push_str(&signing::uri_encode(&path, false));

        // Append to url_path
        #[allow(clippy::collapsible_match)]
        match self.command() {
            Command::InitiateMultipartUpload { .. } | Command::ListMultipartUploads { .. } => {
                url_str.push_str("?uploads")
            }
            Command::AbortMultipartUpload { upload_id } => {
                write!(url_str, "?uploadId={}", upload_id).expect("Could not write to url_str");
            }
            Command::CompleteMultipartUpload { upload_id, .. } => {
                write!(url_str, "?uploadId={}", upload_id).expect("Could not write to url_str");
            }
            Command::GetObjectTorrent => url_str.push_str("?torrent"),
            Command::PutObject { multipart, .. } => {
                if let Some(multipart) = multipart {
                    url_str.push_str(&multipart.query_string())
                }
            }
            _ => {}
        }

        let mut url = Url::parse(&url_str)?;

        for (key, value) in &self.bucket().extra_query {
            url.query_pairs_mut().append_pair(key, value);
        }

        if let Command::ListObjectsV2 {
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

        if let Command::ListObjects {
            prefix,
            delimiter,
            marker,
            max_keys,
        } = self.command().clone()
        {
            let mut query_pairs = url.query_pairs_mut();
            delimiter.map(|d| query_pairs.append_pair("delimiter", &d));

            query_pairs.append_pair("prefix", &prefix);
            if let Some(marker) = marker {
                query_pairs.append_pair("marker", &marker);
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

        Ok(url)
    }

    fn canonical_request(&self, headers: &HeaderMap) -> Result<String, S3Error> {
        signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.url()?,
            headers,
            &self.command().sha256(),
        )
    }

    fn authorization(&self, headers: &HeaderMap) -> Result<String, S3Error> {
        let canonical_request = self.canonical_request(headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request)?;
        let mut hmac = signing::HmacSha256::new_from_slice(&self.signing_key()?)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let signed_header = signing::signed_header_string(headers);
        signing::authorization_header(
            &self.bucket().access_key()?.expect("No access_key provided"),
            &self.datetime(),
            &self.bucket().region(),
            &signed_header,
            &signature,
        )
    }

    fn headers(&self) -> Result<HeaderMap, S3Error> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.command().sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.

        let mut headers = HeaderMap::new();

        for (k, v) in self.bucket().extra_headers.iter() {
            headers.insert(k.clone(), v.clone());
        }

        let host_header = self.host_header();

        headers.insert(HOST, host_header.parse()?);

        match self.command() {
            Command::CopyObject { from } => {
                headers.insert(HeaderName::from_static("x-amz-copy-source"), from.parse()?);
            }
            Command::ListObjects { .. } => {}
            Command::ListObjectsV2 { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}
            _ => {
                headers.insert(
                    CONTENT_LENGTH,
                    self.command().content_length().to_string().parse()?,
                );
                headers.insert(CONTENT_TYPE, self.command().content_type().parse()?);
            }
        }
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            sha256.parse()?,
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            self.long_date()?.parse()?,
        );

        if let Some(session_token) = self.bucket().session_token()? {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                session_token.parse()?,
            );
        } else if let Some(security_token) = self.bucket().security_token()? {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                security_token.parse()?,
            );
        }

        if let Command::PutObjectTagging { tags } = self.command() {
            let digest = md5::compute(tags);
            let hash = base64::encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::PutObject { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::UploadPart { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = base64::encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::GetObject {} = self.command() {
            headers.insert(ACCEPT, "application/octet-stream".to_string().parse()?);
        // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        } else if let Command::GetObjectRange { start, end } = self.command() {
            headers.insert(ACCEPT, "application/octet-stream".to_string().parse()?);

            let mut range = format!("bytes={}-", start);

            if let Some(end) = end {
                range.push_str(&end.to_string());
            }

            headers.insert(RANGE, range.parse()?);
        } else if let Command::CreateBucket { ref config } = self.command() {
            config.add_headers(&mut headers)?;
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket().secret_key()?.is_some() {
            let authorization = self.authorization(&headers)?;
            headers.insert(AUTHORIZATION, authorization.parse()?);
        }

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(DATE, self.datetime().format(&Rfc2822)?.parse()?);

        Ok(headers)
    }
}
