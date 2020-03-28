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
extern crate futures;
extern crate core;
extern crate tokio;

pub mod bucket;
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
    Hmac@hmac::crypto_mac::InvalidKeyLength;
    Utf8@std::str::Utf8Error;
    Io@std::io::Error;
    Region@awsregion::AwsRegionError;
    Creds@awscreds::AwsCredsError;
});

const LONG_DATE: &str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
