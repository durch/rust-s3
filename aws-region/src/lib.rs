#![allow(unused_imports)]
#![forbid(unsafe_code)]

simpl::err!(AwsRegionError, {
    Utf8@std::str::Utf8Error;
});

mod region;

pub use region::*;
