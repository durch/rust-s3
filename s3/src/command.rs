use crate::serde_types::CompleteMultipartUploadData;
use reqwest::header::HeaderMap;

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
        custom_headers: Option<HeaderMap>
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
}
