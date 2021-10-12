[![](https://camo.githubusercontent.com/2fee3780a8605b6fc92a43dab8c7b759a274a6cf/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f72757374632d737461626c652d627269676874677265656e2e737667)](https://www.rust-lang.org/downloads.html)
[![build](https://github.com/durch/rust-s3/workflows/build/badge.svg)](https://github.com/durch/rust-s3/actions)
[![](http://meritbadge.herokuapp.com/rust-s3)](https://crates.io/crates/rust-s3)
![](https://img.shields.io/crates/d/rust-s3.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rust-s3/blob/master/LICENSE.md)
<!-- [![Join the chat at https://gitter.im/durch/rust-s3](https://badges.gitter.im/durch/rust-s3.svg)](https://gitter.im/durch/rust-s3?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) -->
## rust-s3 [[docs](https://docs.rs/rust-s3/)]

Rust library for working with Amazon S3 or arbitrary S3 compatible APIs, fully compatible with **async/await** and `futures ^0.3`. All `async` features can be turned off and sync only implementations can be used.

### Support further development

+ BTC - `3QQdtQGSMStTWEBhe65hPiAWJekXH8n26o`
+ ETH - `0x369Fd06ACc25CCfE0A28BE40018cF3aC38AcdcB6`

### Intro

Modest interface towards Amazon S3, as well as S3 compatible object storage APIs such as Wasabi, Yandex, Minio or Google Cloud Storage.
Supports: `put`, `get`, `list`, `delete`, operations on `tags` and `location`, well as `head`. 

Additionally a dedicated `presign_get` `Bucket` method is available. This means you can upload to s3, and give the link to select people without having to worry about publicly accessible files on S3. This also means that you can give people 
a `PUT` presigned URL, meaning they can upload to a specific key in S3 for the duration of the presigned URL.

**[AWS, Yandex and Custom (Minio) Example](https://github.com/durch/rust-s3/blob/master/s3/bin/simple_crud.rs)**

#### Features

There are a lot of various featuers that enable a wide variaty of use cases, refer to `s3/Cargo.toml` for an exhaustive list. Below is a table of various useful features as well as a short description for each.

+ `default` - `tokio` runtime and a `native-tls` implementation
+ `blocking` - generates `*_blocking` variant of all `Bucket` methods, otherwise only `async` versions are available
+ `fail-on-err` - `panic` on any error
+ `no-verify-ssl` - disable SSL verification for endpoints, useful for custom regions
+ `never-encode-slash` - never encode slashes in paths

##### with `default-features = false`

+ `with-async-std` - `async-std` runtime
+ `sync` - no async runtime, `attohttpc` is used for HTTP requests
+ `tags` - required for `Bucket::get_object_tagging`

All runtimes support either `native-tls` or `rustls-tls`, there are features for all combinations, refer to `s3/Cargo.toml` for a complete list

#### Path or subdomain style URLs and headers

`Bucket` struct provides constructors for `path-style` paths, `subdomain` style is the default. `Bucket` exposes methods for configuring and accessing `path-style` configuration. `blocking` feature will generate a `*_blocking` variant of all of the methods listed below.

#### Buckets

|          |                                                                             |
|----------|-----------------------------------------------------------------------------|
| `create` | [async](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.create) |
| `delete` | [async](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.delete) |

#### Presign

|       |                                                                                        |
|-------|----------------------------------------------------------------------------------------|
| `PUT` | [presign_put](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.presign_put) |
| `GET` | [presign_get](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.presign_get) |

#### GET

There are a few different options for getting an object. `sync` and `async` methods are generic over `std::io::Write`,
while `tokio` methods are generic over `tokio::io::AsyncWriteExt`.

|         |                                                                                                    |
|---------|----------------------------------------------------------------------------------------------------|
| `async` | [get_object](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.get_object)               |
| `async` | [get_object_stream](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.get_object_stream) |

#### PUT

Each `GET` method has a `PUT` companion `sync` and `async` methods are generic over `std::io::Read`. `async` `stream` methods are generic over `futures::io::AsyncReadExt`, while `tokio` methods are generic over `tokio::io::AsyncReadExt`.

|         |                                                                                                                          |
|---------|--------------------------------------------------------------------------------------------------------------------------|
| `async` | [put_object](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.put_object)                                     |
| `async` | [put_object_with_content_type](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.put_object_with_content_type) |
| `async` | [put_object_stream](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.put_object_stream)                       |

#### List

|         |                                                                          |
|---------|--------------------------------------------------------------------------|
| `async` | [list](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.list) |

#### DELETE

|         |                                                                                            |
|---------|--------------------------------------------------------------------------------------------|
| `async` | [delete_object](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.delete_object) |

#### Location

|         |                                                                                  |
|---------|----------------------------------------------------------------------------------|
| `async` | [location](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.location) |

#### Tagging

|         |                                                                                                      |
|---------|------------------------------------------------------------------------------------------------------|
| `async` | [put_object_tagging](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.put_object_tagging) |
| `async` | [get_object_tagging](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.get_object_tagging) |

#### Head

|         |                                                                                        |
|---------|----------------------------------------------------------------------------------------|
| `async` | [head_object](https://docs.rs/rust-s3/latest/s3/bucket/struct.Bucket.html#method.head_object) |

### Usage (in `Cargo.toml`)

```toml
[dependencies]
rust-s3 = "0.27"
```

