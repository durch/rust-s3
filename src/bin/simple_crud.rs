extern crate s3;
extern crate rand;

use std::str;

use s3::bucket::Bucket;
use s3::credentials::Credentials;
use s3::error::S3Error;

const BUCKET: &str = "rust-s3-test";
const MESSAGE: &str = "I want to go to S3";
const REGION: &str = "eu-central-1";
const CREDENTIALS_PROFILE: &str = "rust-s3";

pub fn main() -> Result<(), S3Error> {
    let region = REGION.parse()?;
    //     Create Bucket in REGION for BUCKET
    let credentials = Credentials::from_profile(Some(CREDENTIALS_PROFILE.to_string()))?;
    let bucket = Bucket::new(BUCKET, region, credentials)?;

    // List out contents of directory
    let results = bucket.list_all("".to_string(), None)?;
    for (list, code) in results {
        assert_eq!(200, code);
        println!("{:?}", list.contents.len());
    }

    // Make sure that our "test_file" doesn't exist, delete it if it does. Note
    // that the s3 library returns the HTTP code even if it indicates a failure
    // (i.e. 404) since we can't predict desired usage. For example, you may
    // expect a 404 to make sure a fi le doesn't exist.
    //    let (_, code) = bucket.delete("test_file")?;
    //    assert_eq!(204, code);

    // Put a "test_file" with the contents of MESSAGE at the root of the
    // bucket.
    let (_, code) = bucket.put_object("test_file", MESSAGE.as_bytes(), "text/plain")?;
    assert_eq!(200, code);

    // Get the "test_file" contents and make sure that the returned message
    // matches what we sent.
    let (data, code) = bucket.get_object("test_file")?;
    let string = str::from_utf8(&data)?;
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);

    //  Get bucket location
    println!("{:?}", bucket.location()?);

    bucket.put_object_tagging("test_file", &[("test", "tag")])?;
    println!("Tags set");
    let (tags, _status) = bucket.get_object_tagging("test_file")?;
    println!("{:?}", tags);

    // Test with random byte array
    
    let random_bytes: Vec<u8> = (0..3072).map(|_| { rand::random::<u8>() }).collect();
    let (_, code) = bucket.put_object("random.bin", random_bytes.as_slice(), "application/octet-stream")?;
    assert_eq!(200, code);
    let (data, code) = bucket.get_object("random.bin")?;
    assert_eq!(code , 200);
    assert_eq!(data.len(), 3072);
    assert_eq!(data, random_bytes);

    Ok(())
}
