extern crate s3;

use std::env;
use std::str;

use s3::bucket::Bucket;
use s3::credentials::Credentials;

const BUCKET: &'static str = "example-bucket";
const REGION: &'static str = "us-east-1";
const MESSAGE: &'static str = "I want to go to S3";

fn load_credentials() -> Credentials {
    let aws_access = env::var("AWS_KEY_ID").expect("Must specify AWS_ACCESS_KEY_ID");
    let aws_secret = env::var("AWS_SECRET_KEY").expect("Must specify AWS_SECRET_ACCESS_KEY");
    Credentials::new(&aws_access, &aws_secret, None)
}

pub fn main() {
    // Create Bucket in REGION for BUCKET
    let credentials = load_credentials();
    let region = REGION.parse().unwrap();
    let bucket = Bucket::new(BUCKET, region, credentials);

    // List out contents of directory
    let results = bucket.list("", None).unwrap();
    for (list, code) in results {
        assert_eq!(200, code);
        println!("{:?}", list.contents.len());
    }
    

    // Make sure that our "test_file" doesn't exist, delete it if it does. Note
    // that the s3 library returns the HTTP code even if it indicates a failure
    // (i.e. 404) since we can't predict desired usage. For example, you may
    // expect a 404 to make sure a file doesn't exist.
    // let (_, code) = bucket.delete("test_file").unwrap();
    // assert_eq!(204, code);

    // // Put a "test_file" with the contents of MESSAGE at the root of the
    // // bucket.
    let (_, code) = bucket.put("test_file", MESSAGE.as_bytes(), "text/plain").unwrap();
    assert_eq!(200, code);

    // // Get the "test_file" contents and make sure that the returned message
    // // matches what we sent.
    let (data, code) = bucket.get("test_file").unwrap();
    let string = str::from_utf8(&data).unwrap();
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);
}
