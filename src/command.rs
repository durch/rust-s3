use reqwest::Method;

pub enum Command<'a> {
    Put {
        content: &'a [u8],
        content_type: &'a str,
    },
    Get,
    Delete,
    List {
        prefix: &'a str,
        delimiter: Option<&'a str>,
        continuation_token: Option<&'a str>
    },
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> Method {
        match *self {
            Command::Get | Command::List { .. } => Method::GET,
            Command::Put { .. } => Method::PUT,
            Command::Delete => Method::DELETE,
        }
    }
}
