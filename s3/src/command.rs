use crate::serde_types::CompleteMultipartUploadData;
use reqwest::Method;
use reqwest::header::HeaderMap;

#[derive(Clone, Debug)]
pub enum Command<'a> {
    HeadObject,
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
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::GetObject
            | Command::ListBucket { .. }
            | Command::GetBucketLocation
            | Command::GetObjectTagging
            | Command::PresignGet { .. } => Method::GET,
            Command::PutObject { .. }
            | Command::PutObjectTagging { .. }
            | Command::PresignPut { .. }
            | Command::UploadPart { .. } => Method::PUT,
            Command::DeleteObject
            | Command::DeleteObjectTagging
            | Command::AbortMultipartUpload { .. } => Method::DELETE,
            Command::InitiateMultipartUpload | Command::CompleteMultipartUpload { .. } => {
                Method::POST
            }
            Command::HeadObject => {
                Method::HEAD
            }
        }
    }
}
