#![allow(unused_imports)]

simpl::err!(AwsRegionError, {
    Utf8@std::str::Utf8Error;
});

mod region;

pub use region::*;
