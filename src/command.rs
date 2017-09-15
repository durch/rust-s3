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
    pub fn http_verb(&self) -> &'static str {
        match *self {
            Command::Get => "GET",
            Command::Put { .. } => "PUT",
            Command::Delete => "DELETE",
            Command::List { .. } => "GET",
        }
    }
}
