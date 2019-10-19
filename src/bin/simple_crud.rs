extern crate s3;

use std::str;

use s3::bucket::Bucket;
use s3::credentials::Credentials;
use s3::error::S3Error;

const BUCKET: &str = "drazen-test-bucket-2";
const MESSAGE: &str = "I want to go to S3";
const REGION: &str = "us-east-1";

pub fn main() -> Result<(), S3Error> {
    let region = REGION.parse()?;
//     Create Bucket in REGION for BUCKET
    let credentials = Credentials::default();
    let bucket = Bucket::new(BUCKET, region, credentials)?;

    // List out contents of directory
    let results = bucket.list_all("".to_string(), None).unwrap();
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
    let string = str::from_utf8(&data).unwrap();
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);

//  Get bucket location
    println!("{:?}", bucket.location()?);

    bucket.put_object_tagging("test_file", &[("test", "tag")])?;
    println!("Tags set");
    let (tags, _status) = bucket.get_object_tagging("test_file")?;
    println!("{:?}", tags);

    Ok(())
}
