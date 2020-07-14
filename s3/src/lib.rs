//! Simple access to Amazon Web Service's (AWS) Simple Storage Service (S3)
#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_derive;
use serde_xml_rs as serde_xml;
#[macro_use]
extern crate log;


pub use awsregion as region;
pub use awscreds as creds;

pub mod bucket;
pub mod command;
pub mod serde_types;
pub mod signing;
pub mod deserializer;
#[cfg(any(feature = "async", feature = "async-rustls"))]
pub mod request_async;
#[cfg(any(feature = "sync", feature = "sync-rustls", feature = "wasm"))]
pub mod request_sync;

#[cfg(any(feature = "async", feature = "async-rustls"))]
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
    UrlParse@url::ParseError;
});

#[cfg(any(feature = "sync", feature = "sync-rustls", feature = "wasm"))]
simpl::err!(S3Error, {
    Xml@serde_xml::Error;
    Hmac@hmac::crypto_mac::InvalidKeyLength;
    Utf8@std::str::Utf8Error;
    Io@std::io::Error;
    Region@awsregion::AwsRegionError;
    Creds@awscreds::AwsCredsError;
    UrlParse@url::ParseError;
    Http@http::header::InvalidHeaderValue;
    Atto@attohttpc::Error;
});

const LONG_DATE: &str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
