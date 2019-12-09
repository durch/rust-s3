use simpl::err;

err!(S3Error, 
    {
        Xml@serde_xml::Error;
        Req@reqwest::Error;
        ReqHN@reqwest::header::InvalidHeaderName;
        ReqHV@reqwest::header::InvalidHeaderValue;
        Var@std::env::VarError;
        Ini@ini::ini::Error;
        Hmac@hmac::crypto_mac::InvalidKeyLength;
});
