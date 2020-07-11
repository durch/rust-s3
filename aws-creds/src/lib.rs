#![allow(unused_imports)]
#![forbid(unsafe_code)]

simpl::err!(AwsCredsError, {
    Utf8@std::str::Utf8Error;
    Reqwest@reqwest::Error;
    Env@std::env::VarError;
    Ini@ini::ini::Error;
});

mod credentials;

pub use credentials::*;
