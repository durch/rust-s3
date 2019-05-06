use reqwest::Method;

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
        prefix: &'a str,
        delimiter: Option<&'a str>,
        continuation_token: Option<&'a str>
    },
    GetBucketLocation
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::GetObject | Command::ListBucket { .. } | Command::GetBucketLocation | Command::GetObjectTagging => Method::GET,
            Command::PutObject { .. } | Command::PutObjectTagging { .. } => Method::PUT,
            Command::DeleteObject | Command::DeleteObjectTagging => Method::DELETE,
        }
    }
}
