
use std::fmt;

pub enum Method {
    Get,
    Put,
    Delete,
}

// #[cfg(features = "async")]
// impl Into<reqwest::Method> for Method {
//     fn into(self) -> reqwest::Method {
//         match self {
//             Method::Get => reqwest::Method::GET,
//             Method::Put => reqwest::Method::PUT,
//             Method::Delete => reqwest::Method::DELETE,
//         }
//     }
// }

impl Into<http::method::Method> for Method {
    fn into(self) -> http::method::Method {
        match self {
            Method::Get => http::method::Method::GET,
            Method::Put => http::method::Method::PUT,
            Method::Delete => http::method::Method::DELETE,
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Method::Get => write!(f, "GET"),
            Method::Put => write!(f, "PUT"),
            Method::Delete => write!(f, "DELETE"),
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
    },
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::GetObject
            | Command::ListBucket { .. }
            | Command::GetBucketLocation
            | Command::GetObjectTagging
            | Command::PresignGet { .. } => Method::Get,
            Command::PutObject { .. }
            | Command::PutObjectTagging { .. }
            | Command::PresignPut { .. } => Method::Put,
            Command::DeleteObject | Command::DeleteObjectTagging => Method::Delete,
        }
    }
}
