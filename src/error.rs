

simpl::err!(S3Error, {
    Xml@serde_xml::Error;
    Req@reqwest::Error;
    InvalidHeaderName@reqwest::header::InvalidHeaderName;
    InvalidHeaderValue@reqwest::header::InvalidHeaderValue;
    Env@std::env::VarError;
    Ini@ini::ini::Error;
    Hmac@hmac::crypto_mac::InvalidKeyLength;
    Utf8@std::str::Utf8Error;
});
