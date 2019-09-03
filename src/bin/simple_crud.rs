extern crate s3;
extern crate snafu;

use snafu::{ResultExt, Snafu};

use std::str;

use s3::bucket::Bucket;
use s3::credentials::Credentials;

const BUCKET: &str = "drazen-test-bucket-2";
const MESSAGE: &str = "I want to go to S3";
const REGION: &str = "us-east-1";

#[derive(Debug, Snafu)]
pub enum Error {
    InvalidRegion { source: s3::region::Error },
    BucketCreate { source: s3::bucket::Error },
    BucketPut { source: s3::bucket::Error },
    BucketGet { source: s3::bucket::Error },
    BucketPutTag { source: s3::bucket::Error },
    BucketGetTag { source: s3::bucket::Error },
    BucketLocation { source: s3::bucket::Error },
}

type S3Result<T, E = Error> = std::result::Result<T, E>;

pub fn main() -> S3Result<()> {
    let region = REGION.parse().context(InvalidRegion)?;
//     Create Bucket in REGION for BUCKET
    let credentials = Credentials::default();
    let bucket = Bucket::new(BUCKET, region, credentials).context(BucketCreate)?;

    // List out contents of directory
//    let results = bucket.list("", None).unwrap();
//    for (list, code) in results {
//        assert_eq!(200, code);
//        println!("{:?}", list.contents.len());
//    }


    // Make sure that our "test_file" doesn't exist, delete it if it does. Note
    // that the s3 library returns the HTTP code even if it indicates a failure
    // (i.e. 404) since we can't predict desired usage. For example, you may
    // expect a 404 to make sure a fi le doesn't exist.
//    let (_, code) = bucket.delete("test_file")?;
//    assert_eq!(204, code);

    // Put a "test_file" with the contents of MESSAGE at the root of the
    // bucket.
    let (_, code) = bucket.put_object("test_file", MESSAGE.as_bytes(), "text/plain").context(BucketPut)?;
    assert_eq!(200, code);

    // Get the "test_file" contents and make sure that the returned message
    // matches what we sent.
    let (data, code) = bucket.get_object("test_file").context(BucketGet)?;
    let string = str::from_utf8(&data).unwrap();
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);

//  Get bucket location
    println!("{:?}", bucket.location().context(BucketLocation)?);

    bucket.put_object_tagging("test_file", &[("test", "tag")]).context(BucketPutTag)?;
    println!("Tags set");
    let (tags, _status) = bucket.get_object_tagging("test_file").context(BucketGetTag)?;
    println!("{:?}", tags);

    Ok(())
}
