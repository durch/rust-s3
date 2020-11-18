//! Simple access to Amazon Web Service's (AWS) Simple Storage Service (S3)
#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_derive;

use serde_xml_rs as serde_xml;

pub use awscreds as creds;
pub use awsregion as region;

pub use bucket::Bucket;
pub use bucket_ops::BucketConfiguration;
pub use region::Region;

#[cfg(feature = "sync")]
pub mod blocking;
pub mod bucket;
pub mod bucket_ops;
pub mod command;
pub mod deserializer;
#[cfg(feature = "with-tokio")]
pub mod request;
pub mod serde_types;
pub mod signing;
#[cfg(feature = "with-async-std")]
pub mod surf_request;

pub mod request_trait;
pub mod utils;

simpl::err!(S3Error, {
    Xml@serde_xml::Error;
    HttpHeader@http::header::ToStrError;
    Hmac@hmac::crypto_mac::InvalidKeyLength;
    Utf8@std::str::Utf8Error;
    Io@std::io::Error;
    Region@awsregion::AwsRegionError;
    Creds@awscreds::AwsCredsError;
    UrlParse@url::ParseError;
});

const LONG_DATE: &str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
