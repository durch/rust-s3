[![](https://camo.githubusercontent.com/2fee3780a8605b6fc92a43dab8c7b759a274a6cf/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f72757374632d737461626c652d627269676874677265656e2e737667)](https://www.rust-lang.org/downloads.html)
[![](https://travis-ci.org/durch/rust-s3.svg?branch=master)](https://travis-ci.org/durch/rust-s3)
[![](http://meritbadge.herokuapp.com/rust-s3)](https://crates.io/crates/rust-s3)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rust-s3/blob/master/LICENSE.md)
[![Join the chat at https://gitter.im/durch/rust-s3](https://badges.gitter.im/durch/rust-s3.svg)](https://gitter.im/durch/rust-s3?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
## rust-s3 [[docs](https://durch.github.io/rust-s3/)]

Tiny Rust library for working with Amazon S3

### Intro
Very modest interface towards Amazon S3.
Supports `put`, `get`, `list`, and `delete`.

### What is cool

The main (and probably only) cool feature is that `put` commands return a presigned link to the file you uploaded.
This means you can upload to s3, and give the link to select people without having to worry about publicly accessible files on S3.

### Configuration

Getter and setter functions exist for all `Link` params... You don't really have to touch anything there, maybe `amz-expire`,
it is configured for one week which is the maximum Amazon allows ATM.

### Usage

*In your Cargo.toml*

```
[dependencies]
rust-s3 = "0.5.0"
```

#### Example

```rust
extern crate s3;

use std::env;
use std::str;

use s3::bucket::Bucket;
use s3::credentials::Credentials;

const BUCKET: &'static str = "example-bucket";
const REGION: &'static str = "us-east-1";
const MESSAGE: &'static str = "I want to go to S3";

fn load_credentials() -> Credentials {
    let aws_access = env::var("AWS_ACCESS_KEY_ID").expect("Must specify AWS_ACCESS_KEY_ID");
    let aws_secret = env::var("AWS_SECRET_ACCESS_KEY").expect("Must specify AWS_SECRET_ACCESS_KEY");
    Credentials::new(&aws_access, &aws_secret, None)
}

pub fn main() {
    // Create Bucket in REGION for BUCKET
    let credentials = load_credentials();
    let region = REGION.parse().unwrap();
    let bucket = Bucket::new(BUCKET, region, credentials);

    // List out contents of directory
    let (list, code) = bucket.list("", None).unwrap();
    assert_eq!(200, code);
    println!("{:?}", list);

    // Make sure that our "test_file" doesn't exist, delete it if it does. Note
    // that the s3 library returns the HTTP code even if it indicates a failure
    // (i.e. 404) since we can't predict desired usage. For example, you may
    // expect a 404 to make sure a file doesn't exist.
    let (_, code) = bucket.delete("test_file").unwrap();
    assert_eq!(204, code);

    // Put a "test_file" with the contents of MESSAGE at the root of the
    // bucket.
    let (_, code) = bucket.put("test_file", MESSAGE.as_bytes(), "text/plain").unwrap();
    assert_eq!(200, code);

    // Get the "test_file" contents and make sure that the returned message
    // matches what we sent.
    let (data, code) = bucket.get("test_file").unwrap();
    let string = str::from_utf8(&data).unwrap();
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);
}
```

