use std;
use serde_xml;
use reqwest;
use core::fmt;
use std::error::Error;

macro_rules! impl_from {
    ($t: ty) => {
        impl From<$t> for S3Error {
            fn from(e: $t) -> S3Error {
                S3Error { src: Some(String::from(format!("{}",e))) }
            }
        }
    }
}

impl_from!(serde_xml::Error);
impl_from!(reqwest::Error);
impl_from!(reqwest::header::InvalidHeaderName);
impl_from!(reqwest::header::InvalidHeaderValue);
impl_from!(std::env::VarError);
impl_from!(ini::ini::Error);
impl_from!(hmac::crypto_mac::InvalidKeyLength);

#[derive(Debug)]
pub struct S3Error {
    pub src: Option<String>
}

pub fn err(e: &str) -> S3Error {
    S3Error {src: Some(String::from(e))}
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.src.as_ref() {
            Some(err) => write!(f, "{}", err),
            None => write!(f, "An unknown error has occured!")
        }

    }
}

impl Error for S3Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

pub type S3Result<T, E = S3Error> = std::result::Result<T, E>;