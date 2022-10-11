// cargo run --example google-cloud

use awscreds::Credentials;
use awsregion::Region;
use s3::error::S3Error;
use s3::Bucket;

#[tokio::main]
async fn main() -> Result<(), S3Error> {
    // This requires a running minio server at localhost:9000
    let bucket = Bucket::new(
        "test-rust-s3",
        Region::Custom {
            region: "us-east1".to_owned(),
            endpoint: "https://storage.googleapis.com".to_owned(),
        },
        Credentials::default()?,
    )?
    .with_path_style();

    let s3_path = "test.file";
    let test = b"I'm going to S3!";

    let response_data = bucket.put_object(s3_path, test).await?;
    assert_eq!(response_data.status_code(), 200);

    let response_data = bucket.get_object(s3_path).await?;
    assert_eq!(response_data.status_code(), 200);
    assert_eq!(test, response_data.as_slice());

    let response_data = bucket
        .get_object_range(s3_path, 100, Some(1000))
        .await
        .unwrap();
    assert_eq!(response_data.status_code(), 206);
    let (head_object_result, code) = bucket.head_object(s3_path).await?;
    assert_eq!(code, 200);
    assert_eq!(
        head_object_result.content_type.unwrap_or_default(),
        "application/octet-stream".to_owned()
    );

    let response_data = bucket.delete_object(s3_path).await?;
    assert_eq!(response_data.status_code(), 204);
    Ok(())
}
