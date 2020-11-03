use crate::serde_types::CompleteMultipartUploadData;
use crate::bucket::Headers;

use sha2::{Digest, Sha256};
use crate::EMPTY_PAYLOAD_SHA;

pub enum HttpMethod {
    Delete,
    Get,
    Put,
    Post
}

use std::fmt;

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpMethod::Delete => write!(f, "DELETE"),
            HttpMethod::Get => write!(f, "GET"),
            HttpMethod::Post => write!(f, "POST"),
            HttpMethod::Put => write!(f, "PUT")
        }
    }
}

#[derive(Clone, Debug)]
pub enum Command<'a> {
    DeleteObject,
    DeleteObjectTagging,
    GetObject,
    GetObjectTagging,
    PutObject {
        content: &'a [u8],
        content_type: &'a str,
    },
    PutObjectTagging {
        tags: &'a str,
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
        custom_headers: Option<Headers>
    },
    InitiateMultipartUpload,
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
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> HttpMethod {
        match *self {
            Command::GetObject
            | Command::ListBucket { .. }
            | Command::GetBucketLocation
            | Command::GetObjectTagging
            | Command::PresignGet { .. } => HttpMethod::Get,
            Command::PutObject { .. }
            | Command::PutObjectTagging { .. }
            | Command::PresignPut { .. }
            | Command::UploadPart { .. } => HttpMethod::Put,
            Command::DeleteObject
            | Command::DeleteObjectTagging
            | Command::AbortMultipartUpload { .. } => HttpMethod::Delete,
            Command::InitiateMultipartUpload | Command::CompleteMultipartUpload { .. } => {
                HttpMethod::Post
            }
        }
    }

    pub fn content_length(&self) -> usize {
        match &self {
            Command::PutObject { content, .. } => content.len(),
            Command::PutObjectTagging { tags } => tags.len(),
            Command::UploadPart { content, .. } => content.len(),
            Command::CompleteMultipartUpload { data, .. } => data.len(),
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
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }
}
