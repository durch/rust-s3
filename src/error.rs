use std;
use serde_xml;
use reqwest;
use core::fmt;

// crate should be called simpl, than we have simpl::from and simpl:err

macro_rules! from {
    ($t: ty) => {
        impl From<$t> for S3Error {
            fn from(e: $t) -> S3Error {
                S3Error { description: Some(String::from(format!("{}",e))) }
            }
        }
    }
}

macro_rules! err {
    ($i: ident) => {
        #[derive(Debug)]
        pub struct $i {
            pub description: Option<String>
        }

        impl From<&str> for $i {
            fn from(str: &str) -> Self {
                $i { description: Some(str.to_string())}
            }
        }

        impl fmt::Display for S3Error {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.description.as_ref() {
                    Some(err) => write!(f, "{}", err),
                    None => write!(f, "An unknown error has occurred!")
                }

            }
        }
    }
}

err!(S3Error);
from!(serde_xml::Error);
from!(reqwest::Error);
from!(reqwest::header::InvalidHeaderName);
from!(reqwest::header::InvalidHeaderValue);
from!(std::env::VarError);
from!(ini::ini::Error);
from!(hmac::crypto_mac::InvalidKeyLength);

pub type S3Result<T, E = S3Error> = std::result::Result<T, E>;