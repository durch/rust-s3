#![allow(unused_imports)]
#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_derive;

simpl::err!(AwsCredsError, {
    Utf8@std::str::Utf8Error;
    Reqwest@attohttpc::Error;
    Env@std::env::VarError;
    Ini@ini::Error;
    Io@std::io::Error;
});

mod credentials;

pub use credentials::*;
