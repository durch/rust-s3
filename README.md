[![](https://camo.githubusercontent.com/2fee3780a8605b6fc92a43dab8c7b759a274a6cf/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f72757374632d737461626c652d627269676874677265656e2e737667)](https://www.rust-lang.org/downloads.html)
[![](https://travis-ci.org/durch/rust-s3.svg?branch=master)](https://travis-ci.org/durch/rust-s3)
[![](http://meritbadge.herokuapp.com/rust-s3)](https://crates.io/crates/rust-s3)
![](https://img.shields.io/crates/d/rust-s3.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rust-s3/blob/master/LICENSE.md)
[![Join the chat at https://gitter.im/durch/rust-s3](https://badges.gitter.im/durch/rust-s3.svg)](https://gitter.im/durch/rust-s3?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
## rust-s3 [[docs](https://durch.github.io/rust-s3/)]

Tiny Rust library for working with Amazon S3 or arbitrary S3 compatible APIs

### Intro
Very modest interface towards Amazon S3.
Supports `put`, `get`, `list`, and `delete`.

### What is cool

The main cool feature is that `put` commands return a presigned link to the file you uploaded.
This means you can upload to s3, and give the link to select people without having to worry about publicly accessible files on S3.

Also supports streaming S3 contents generic over `T: Write` (`0.14.0-beta.0`).

### Configuration

Getter and setter functions exist for all `Link` params... You don't really have to touch anything there, maybe `amz-expire`,
it is configured for one week which is the maximum Amazon allows ATM.

### Usage

*In your Cargo.toml*

```
[dependencies]
rust-s3 = "0.13.0"
```

#### Disable SSL verification for endpoints
```
[dependencies]
rust-s3 = {version = "0.13.0", features = ["no-verify-ssl"]}
```

#### Beta version of streaming downloads via `stream_object` Bucket method
```
[dependencies]
rust-s3 = "0.14.0-beta.0"
```

#### Example

+ [Simple S3 CRUD](https://github.com/durch/rust-s3/blob/master/src/bin/simple_crud.rs)

