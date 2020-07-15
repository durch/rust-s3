[![](https://camo.githubusercontent.com/2fee3780a8605b6fc92a43dab8c7b759a274a6cf/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f72757374632d737461626c652d627269676874677265656e2e737667)](https://www.rust-lang.org/downloads.html)
[![build](https://github.com/durch/rust-s3/workflows/build/badge.svg)](https://github.com/durch/rust-s3/actions)
[![](http://meritbadge.herokuapp.com/rust-s3)](https://crates.io/crates/rust-s3)
![](https://img.shields.io/crates/d/rust-s3.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rust-s3/blob/master/LICENSE.md)
## rust-s3 [[docs](https://docs.rs/rust-s3/)]


Rust library for working with Amazon S3 or arbitrary S3 compatible APIs, fully compatible with **async/await** and `futures ^0.3`. Blocking familiy of methods is also compatible with `wasm`.

### Support further development

+ BTC - `3QQdtQGSMStTWEBhe65hPiAWJekXH8n26o`
+ ETH - `0x369Fd06ACc25CCfE0A28BE40018cF3aC38AcdcB6`

### Intro

Modest interface towards Amazon S3, as well as S3 compatible object storage APIs such as Wasabi, Yandex or Minio.
Supports `put`, `get`, `list`, `delete`, operations on `tags` and `location`. 

Additionally a dedicated `presign_get` `Bucket` method is available. This means you can upload to s3, and give the link to select people without having to worry about publicly accessible files on S3. This also means that you can give people 
a `PUT` presigned URL, meaning they can upload to a specific key in S3 for the duration of the presigned URL.

**[AWS, Yandex and Custom (Minio) Example](https://github.com/durch/rust-s3/blob/master/s3/bin/simple_crud.rs)**

#### Path or subdomain style URLs and headers

`Bucket` struct provides constructors for `path-style` paths, `subdomain` style is the default. `Bucket` exposes methods for configuring and accessing `path-style` configuration.

#### Features

`sync` and `async` features are enabled by default. In case you require a different set of features you are probably better off passing in `default-features = false`, see below. Both `sync` and `async` use `native-tls` as their TLS backend, there are `rustls-tls` alternative backends available for both of them. `rustls-tls` features are functionally compatible to their `native-tls` counterparts, and can be toggled individually or together with the `rustls` feature. `wasm` feature is sugar for [`sync-rustls`]. Useful feature combinations are listed at the bottom of this doc.

#### Presign

| **feature**    |                                                                                                |
|----------------|------------------------------------------------------------------------------------------------|
| `sync`, `wasm` | [presign_put](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.presign_put) |
| `sync`, `wasm` | [presign_get](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.presign_get) |

#### GET

There are a few different options for getting an object. `async` and `sync` methods are generic over `std::io::Write`,
while `tokio` methods are generic over `tokio::io::AsyncWriteExt`.

| **feature**    |                                                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------------------------------|
| `async`        | [get_object](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object)                                 |
| `async`        | [get_object_stream](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object_stream)                   |
| `sync`, `wasm` | [get_object_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object_blocking)               |
| `sync`, `wasm` | [get_object_stream_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object_stream_blocking) |
| `async`        | [tokio_get_object_stream](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.tokio_get_object_stream)       |

#### PUT

Each `GET` method has a `PUT` companion `sync` and `async` methods are generic over `std::io::Read`,
while `tokio` methods are generic over `tokio::io::AsyncReadExt`.

| **features**   |                                                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------------------------------|
| `async`        | [put_object](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object)                                 |
| `async`        | [put_object_stream](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object_stream)                   |
| `sync`, `wasm` | [put_object_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object_blocking)               |
| `sync`, `wasm` | [put_object_stream_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object_stream_blocking) |
| `async`        | [tokio_put_object_stream](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.tokio_put_object_stream)       |

#### List

| **features**   |                                                                                                    |
|----------------|----------------------------------------------------------------------------------------------------|
| `async`        | [list](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.list)                   |
| `sync`, `wasm` | [list_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.list_blocking) |

#### DELETE

| **features**   |                                                                                                                      |
|----------------|----------------------------------------------------------------------------------------------------------------------|
| `async`        | [delete_object](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.delete_object)                   |
| `sync`, `wasm` | [delete_object_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.delete_object_blocking) |

#### Location

| **features**   |                                                                                                            |
|----------------|------------------------------------------------------------------------------------------------------------|
| `async`        | [location](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.location)                   |
| `sync`, `wasm` | [location_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.location_blocking) |

#### Tagging

| **features**   |                                                                                                                                |
|----------------|--------------------------------------------------------------------------------------------------------------------------------|
| `async`        | [put_object_tagging](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object_tagging)                   |
| `sync`, `wasm` | [put_object_tagging_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.put_object_tagging_blocking) |
| `async`        | [get_object_tagging](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object_tagging)                   |
| `sync`, `wasm` | [get_object_tagging_blocking](https://durch.github.io/rust-s3/s3/bucket/struct.Bucket.html#method.get_object_tagging_blocking) |

### Usage (in `Cargo.toml`)

```toml
[dependencies]
rust-s3 = "0.23"
```

#### Features

##### Disable SSL verification for endpoints, useful for custom regions

```toml
# Only available for `async` feature, as `rustls-tls` does not support dangereous features ATM
[dependencies]
rust-s3 = {version = "0.23", features = ["async", "no-verify-ssl"], default-features = false}
```

##### Fail on HTTP error responses

```toml
[dependencies]
rust-s3 = {version = "0.23", features = ["fail-on-err"]}
```

##### Different SSL backends

Default is `native-tls`, it is possible to switch to `rustls-tls` which is more portable

```toml
[dependencies]
rust-s3 = {version = "0.23", features = ["rustls"], default-features = false}
```

##### WASM

```toml
[dependencies]
rust-s3 = {version = "0.23", features = ["wasm"], default-features = false}
```

