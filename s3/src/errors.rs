use thiserror::Error;

#[derive(Error, Debug)]
pub enum PresignError {
    #[error("invalid key length")]
    InvalidKeylength,
    #[error("url parsing failed: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("Max expiration for presigned URLs is one week, or 604.800 seconds, got {0} instead")]
    ExpirationOverflow(u32),
}

impl From<crypto_mac::InvalidKeyLength> for PresignError {
    fn from(_: crypto_mac::InvalidKeyLength) -> PresignError {
        PresignError::InvalidKeylength
    }
}

#[derive(Error, Debug)]
pub enum ResponseError {
    #[error("invalid key length when signing the request")]
    SigningError,
    #[error("status code {0} indicates that the request failed:\n{1:?}")]
    /// StatusCode contains the returned HTTP status code along with the body of the response
    StatusCode(u16, Option<String>),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("String conversion failed: {0}")]
    InvalidHeaderString(#[from] http::header::ToStrError),
    #[error("UTF-8 conversion error: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Deserialization error: {0}")]
    ImpossibleDeserialization(#[from] serde_xml_rs::Error),

    #[cfg(feature = "with-tokio")]
    #[error("failed request {0}")]
    RequestError(#[from] reqwest::Error),

    #[cfg(any(feature = "blocking", feature = "sync"))]
    #[error("failed request {0}")]
    RequestError(#[from] attohttpc::Error),
}

impl From<crypto_mac::InvalidKeyLength> for ResponseError {
    fn from(_: crypto_mac::InvalidKeyLength) -> ResponseError {
        ResponseError::SigningError
    }
}
