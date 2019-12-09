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
extern crate simpl;

pub mod bucket;
pub mod credentials;
pub mod command;
pub mod region;
pub mod request;
pub mod serde_types;
pub mod signing;
pub mod deserializer;
#[macro_use]
pub mod error;

const LONG_DATE: &str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[allow(dead_code)]
mod test {
    use super::error::S3Result;
    err!(S3Error);

    #[cfg(test)]
    fn test_error() {
        fn is_error(_e: &dyn std::error::Error) -> S3Result<()> {
            Ok(())
        }

        let err: S3Error = "test error".into();

        is_error(&err).unwrap();
    }
}