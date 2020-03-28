//! Simple access to Amazon Web Service's (AWS) Simple Storage Service (S3)
extern crate chrono;
extern crate hex;
extern crate hmac;
extern crate reqwest;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_xml_rs as serde_xml;
extern crate sha2;
extern crate url;
extern crate ini;
extern crate dirs;
extern crate futures;
extern crate core;
extern crate tokio;

pub mod bucket;
pub mod credentials;
pub mod command;
pub mod request;
pub mod serde_types;
pub mod signing;
pub mod deserializer;

simpl::err!(S3Error, {
    Xml@serde_xml::Error;
    Req@reqwest::Error;
    InvalidHeaderName@reqwest::header::InvalidHeaderName;
    InvalidHeaderValue@reqwest::header::InvalidHeaderValue;
    Env@std::env::VarError;
    Ini@ini::ini::Error;
    Hmac@hmac::crypto_mac::InvalidKeyLength;
    Utf8@std::str::Utf8Error;
    Io@std::io::Error;
    Region@awsregion::AwsRegionError;
});

const LONG_DATE: &str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[allow(dead_code)]
mod test {
    use crate::{S3Error, Result};

    #[cfg(test)]
    fn test_error() {
        fn is_error(_e: &dyn std::error::Error) -> Result<()> {
            Ok(())
        }

        let err: S3Error = "test error".into();

        is_error(&err).unwrap();
    }
}