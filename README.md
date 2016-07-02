[![](https://camo.githubusercontent.com/79318781f189b2ee91c3a150bf27813c386afaf2/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f72757374632d6e696768746c792d79656c6c6f772e737667)
[![](https://travis-ci.org/durch/rust-s3.svg?branch=master)](https://travis-ci.org/durch/rust-s3)
[![](http://meritbadge.herokuapp.com/rust-s3)](https://crates.io/crates/rust-s3)

# rust-s3
Tiny Rust library for working with Amazon S3

*Rust nightly required because of compile time configuration, will likely create a stable branch...*

*Increasingly more loosly based on [crates.io](https://github.com/rust-lang/crates.io/tree/master/src/s3) implementation.*

## Intro 
Very modest interface towards Amazon S3. 
Supports `put`, `get` and `list`, with `delete` on the roadmap and will be done eventually, 
probably around the time I discover I need it in some other project :).

## What is cool

The main (and probably only) cool feature is that `put` commands return a presigned link to the file you uploaded. 
This means you can upload to s3, and give the link to select people without having to worry about publicly accessible files on S3.

## Configuration

Compile time configuration is done using [Config.toml](https://github.com/durch/rust-s3/blob/master/Config.toml), 
curtosy of [confy](https://github.com/Luthaf/confy). You don't really have to touch anything there, maybe `amz-expire`, 
it is configured for one week which is the maximum Amazon allows ATM.

## Usage 

*In your Cargo.toml*

```
[dependencies]
rust-s3 = '0.2.0'
```

#### Example

```
extern crate rust-s3;
use rust-s3::{Bucket, put_s3, get_s3, list_s3};

const S3_BUCKET: &'static str = "bucket_name";
const AWS_ACCESS: &'static str = "access_key";
const AWS_SECRET: &'static str = "secret_key";

fn main () {
  // Bucket instance
  let bucket = Bucket::new(S3_BUCKET.to_string(),
                              None,
                              AWS_ACCESS.to_string(),
                              AWS_SECRET.to_string(),
                              &"https");
  
  // Put
  let put_me = "I want to go to S3".to_string();
  let url = put_s3(&bucket,
                  &"/",
                  &put_me.as_bytes());
  println!("{}", url);
  
  // List
  let bytes = list_s3(&bucket, 
                      &"/", 
                      &"/", 
                      &"/");
  let string = String::from_utf8_lossy(&bytes);
  println!("{}", string);
  
  // Get
  let path = &"test_file";
  let mut buffer = match File::create(path) {
            Ok(x) => x,
            Err(e) => panic!("{:?}, {}", e, path)
          };
  let bytes = get_s3(&bucket, Some(&path));
  match buffer.write(&bytes) {
    Ok(_) => {} // info!("Written {} bytes from {}", x, path),
    Err(e) => panic!("{:?}", e)
  }
}
  ```

