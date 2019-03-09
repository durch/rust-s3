use reqwest::Method;

pub enum Command<'a> {
    Put {
        content: &'a [u8],
        content_type: &'a str,
    },
    Tag {
        tags: &'a str
    },
    GetTags,
    Get,
    Delete,
    List {
        prefix: &'a str,
        delimiter: Option<&'a str>,
        continuation_token: Option<&'a str>
    },
    BucketOpGet
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::Get | Command::List { .. } | Command::BucketOpGet | Command::GetTags => Method::GET,
            Command::Put { .. } | Command::Tag { .. } => Method::PUT,
            Command::Delete => Method::DELETE,
        }
    }
}
