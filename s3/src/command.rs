use reqwest::Method;

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
        tags: &'a str
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
        expiry_secs: u32
    },
    PresignPut {
        expiry_secs: u32
    }
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::GetObject | Command::ListBucket { .. } | Command::GetBucketLocation | Command::GetObjectTagging | Command::PresignGet { .. } => Method::GET,
            Command::PutObject { .. } | Command::PutObjectTagging { .. } | Command::PresignPut { .. } => Method::PUT,
            Command::DeleteObject | Command::DeleteObjectTagging => Method::DELETE,
        }
    }
}
