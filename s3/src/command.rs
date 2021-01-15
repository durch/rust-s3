use crate::serde_types::CompleteMultipartUploadData;

use crate::EMPTY_PAYLOAD_SHA;
use sha2::{Digest, Sha256};

pub enum HttpMethod {
    Delete,
    Get,
    Put,
    Post,
    Head,
}

use std::fmt;

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpMethod::Delete => write!(f, "DELETE"),
            HttpMethod::Get => write!(f, "GET"),
            HttpMethod::Post => write!(f, "POST"),
            HttpMethod::Put => write!(f, "PUT"),
            HttpMethod::Head => write!(f, "HEAD"),
        }
    }
}
use crate::bucket_ops::BucketConfiguration;
use http::HeaderMap;

#[derive(Clone, Debug)]
pub struct Multipart<'a> {
    part_number: u32,
    upload_id: &'a str,
}

impl<'a> Multipart<'a> {
    pub fn query_string(&self) -> String {
        format!(
            "?partNumber={}&uploadId={}",
            self.part_number, self.upload_id
        )
    }

    pub fn new(part_number: u32, upload_id: &'a str) -> Self {
        Multipart {
            part_number,
            upload_id,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Command<'a> {
    HeadObject,
    DeleteObject,
    DeleteObjectTagging,
    GetObject,
    GetObjectRange {
        start: u64,
        end: Option<u64>,
    },
    GetObjectTagging,
    PutObject {
        content: &'a [u8],
        content_type: &'a str,
        multipart: Option<Multipart<'a>>,
        custom_headers: Option<HeaderMap>,
    },
    PutObjectTagging {
        tags: &'a str,
    },
    ListMultipartUploads {
        prefix: Option<&'a str>,
        delimiter: Option<&'a str>,
        key_marker: Option<String>,
        max_uploads: Option<usize>,
    },
    ListBucket {
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<usize>,
    },
    GetBucketLocation,
    PresignGet {
        expiry_secs: u32,
    },
    PresignPut {
        expiry_secs: u32,
        custom_headers: Option<HeaderMap>,
    },
    InitiateMultipartUpload {
        custom_headers: Option<HeaderMap>,
    },
    UploadPart {
        part_number: u32,
        content: &'a [u8],
        upload_id: &'a str,
    },
    AbortMultipartUpload {
        upload_id: &'a str,
    },
    CompleteMultipartUpload {
        upload_id: &'a str,
        data: CompleteMultipartUploadData,
    },
    CreateBucket {
        config: BucketConfiguration,
    },
    DeleteBucket,
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> HttpMethod {
        match *self {
            Command::GetObject
            | Command::GetObjectRange { .. }
            | Command::ListBucket { .. }
            | Command::GetBucketLocation
            | Command::GetObjectTagging
            | Command::ListMultipartUploads { .. }
            | Command::PresignGet { .. } => HttpMethod::Get,
            Command::PutObject { .. }
            | Command::PutObjectTagging { .. }
            | Command::PresignPut { .. }
            | Command::UploadPart { .. }
            | Command::CreateBucket { .. } => HttpMethod::Put,
            Command::DeleteObject
            | Command::DeleteObjectTagging
            | Command::AbortMultipartUpload { .. }
            | Command::DeleteBucket => HttpMethod::Delete,
            Command::InitiateMultipartUpload { .. } | Command::CompleteMultipartUpload { .. } => {
                HttpMethod::Post
            }
            Command::HeadObject => HttpMethod::Head,
        }
    }

    pub fn content_length(&self) -> usize {
        match &self {
            Command::PutObject { content, .. } => content.len(),
            Command::PutObjectTagging { tags } => tags.len(),
            Command::UploadPart { content, .. } => content.len(),
            Command::CompleteMultipartUpload { data, .. } => data.len(),
            Command::CreateBucket { config } => {
                if let Some(payload) = config.location_constraint_payload() {
                    Vec::from(payload).len()
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    pub fn content_type(&self) -> String {
        match self {
            Command::PutObject { content_type, .. } => content_type.to_string(),
            Command::CompleteMultipartUpload { .. } => "application/xml".into(),
            _ => "text/plain".into(),
        }
    }

    pub fn sha256(&self) -> String {
        match &self {
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
            Command::CreateBucket { config } => {
                if let Some(payload) = config.location_constraint_payload() {
                    let mut sha = Sha256::default();
                    sha.update(payload.as_bytes());
                    hex::encode(sha.finalize().as_slice())
                } else {
                    EMPTY_PAYLOAD_SHA.into()
                }
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }
}
