use reqwest::Method;
use std::vec::Vec;

pub enum Command<'a> {
    Put {
        content: &'a [u8],
        content_type: &'a str,
    },
    Tag {
        tags: Vec<(&'a str, &'a str)>
    },
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
            Command::Get | Command::List { .. } | Command::BucketOpGet => Method::GET,
            Command::Put { .. } | Command::Tag { .. } => Method::PUT,
            Command::Delete => Method::DELETE,
        }
    }

    pub fn tags_xml(&self) -> String {
        let mut s = String::new();
        if let Command::Tag { ref tags } = *self {
            let mut content = tags
                .iter()
                .map(|&(name, value)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", name, value))
                .fold(String::new(), |mut a, b| {
                    a.push_str(b.as_str());
                    a
                });
            s.push_str("<Tagging><TagSet>");
            s.push_str(&content);
            s.push_str("</TagSet></Tagging>");
        }

        s
    }
}
