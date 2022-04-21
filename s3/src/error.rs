use thiserror::Error;

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("Utf8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Max expiration for presigned URLs is one week, or 604.800 seconds, got {0} instead")]
    MaxExpiry(u32),
    #[error("Got HTTP {0} with content '{1}'")]
    Http(u16, String),
    #[error("Http request returned a non 2** code")]
    HttpFail,
    #[error("aws-creds: {0}")]
    Credentials(#[from] crate::creds::error::CredentialsError),
    #[error("aws-region: {0}")]
    Region(#[from] crate::region::error::RegionError),
    #[error("sha2 invalid length: {0}")]
    HmacInvalidLength(#[from] sha2::digest::InvalidLength),
    #[error("url parse: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "with-tokio")]
    #[error("reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("header to string: {0}")]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error("from utf8: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("serde xml: {0}")]
    SerdeXml(#[from] serde_xml_rs::Error),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[cfg(feature = "with-async-std")]
    #[error("surf: {0}")]
    Surf(String),
    #[cfg(feature = "sync")]
    #[error("attohttpc: {0}")]
    Atto(#[from] attohttpc::Error),
}
