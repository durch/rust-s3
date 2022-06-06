#![allow(unused_imports)]
#![forbid(unsafe_code)]

#[macro_use]
extern crate serde_derive;

mod credentials;
pub use credentials::*;
pub mod error;

// Reexport for e.g. users who need to build Credentials
pub use time;
