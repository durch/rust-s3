#![allow(unused_imports)]

simpl::err!(AwsCredsError, {
    Utf8@std::str::Utf8Error;
    Reqwest@reqwest::Error;
    Env@std::env::VarError;
    Ini@ini::ini::Error;
});

mod credentials;

pub use credentials::*;
