//! Simple access to Amazon Web Service's (AWS) Simple Storage Service (S3)
#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_derive;

pub use awscreds as creds;
pub use awsregion as region;

pub use bucket::Bucket;
pub use bucket::Tag;
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

pub mod error;
pub mod request_trait;
pub mod utils;

const LONG_DATETIME: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year][month][day]T[hour][minute][second]Z");
const EMPTY_PAYLOAD_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
