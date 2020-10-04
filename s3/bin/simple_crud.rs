extern crate s3;

use std::str;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::S3Error;

struct Storage {
    name: String,
    region: Region,
    credentials: Credentials,
    bucket: String,
    location_supported: bool,
}

const MESSAGE: &str = "I want to go to S3";

pub fn main() -> Result<(), S3Error> {
    let aws = Storage {
        name: "aws".into(),
        region: "eu-central-1".parse()?,
        // credentials: Credentials::from_profile(Some("rust-s3"))?,
        credentials: Credentials::from_env_specific(
            Some("EU_AWS_ACCESS_KEY_ID"),
            Some("EU_AWS_SECRET_ACCESS_KEY"),
            None,
            None,
        )?,
        bucket: "rust-s3-test".to_string(),
        location_supported: true,
    };

    // let aws_public = Storage {
    //     name: "aws-public".into(),
    //     region: "eu-central-1".parse()?,
    //     credentials: Credentials::anonymous()?,
    //     bucket: "rust-s3-public".to_string(),
    //     location_supported: true,
    // };

    // let minio = Storage {
    //     name: "minio".into(),
    //     region: Region::Custom {
    //         region: "us-east-1".into(),
    //         endpoint: "https://minio.adder.black".into(),
    //     },
    //     credentials: Credentials::from_profile(Some("minio"))?,
    //     bucket: "rust-s3".to_string(),
    //     location_supported: false,
    // };

    // let yandex = Storage {
    //     name: "yandex".into(),
    //     region: "ru-central1".parse()?,
    //     credentials: Credentials::from_profile(Some("yandex"))?,
    //     bucket: "soundcloud".to_string(),
    //     location_supported: false,
    // };

    for backend in vec![aws] {
        println!("Running {}", backend.name);
        // Create Bucket in REGION for BUCKET
        let bucket = Bucket::new(&backend.bucket, backend.region, backend.credentials)?;

        // List out contents of directory
        let results = bucket.list_blocking("".to_string(), None)?;
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
        let (_, code) = bucket.put_object_blocking("test_file", MESSAGE.as_bytes())?;
        // println!("{}", bucket.presign_get("test_file", 604801)?);
        assert_eq!(200, code);

        // Get the "test_file" contents and make sure that the returned message
        // matches what we sent.
        let (data, code) = bucket.get_object_blocking("test_file")?;
        let string = str::from_utf8(&data)?;
        // println!("{}", string);
        assert_eq!(200, code);
        assert_eq!(MESSAGE, string);

        if backend.location_supported {
            // Get bucket location
            println!("{:?}", bucket.location_blocking()?);
        }

        bucket.put_object_tagging_blocking("test_file", &[("test", "tag")])?;
        println!("Tags set");
        let (tags, _status) = bucket.get_object_tagging_blocking("test_file")?;
        println!("{:?}", tags);

        // Test with random byte array

        let random_bytes: Vec<u8> = (0..3072).map(|_| 33).collect();
        let (_, code) = bucket.put_object_blocking("random.bin", random_bytes.as_slice())?;
        assert_eq!(200, code);
        let (data, code) = bucket.get_object_blocking("random.bin")?;
        assert_eq!(code, 200);
        assert_eq!(data.len(), 3072);
        assert_eq!(data, random_bytes);
    }

    Ok(())
}
