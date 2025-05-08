// cargo run --example async-std --no-default-features --features async-std-native-tls

#[cfg(feature = "async-std")]
use awscreds::Credentials;
#[cfg(feature = "async-std")]
use s3::error::S3Error;
#[cfg(feature = "async-std")]
use s3::Bucket;

#[cfg(not(feature = "async-std"))]
fn main() {}

#[cfg(feature = "async-std")]
#[async_std::main]
async fn main() -> Result<(), S3Error> {
    let bucket = Bucket::new(
        "rust-s3-test",
        "eu-central-1".parse()?,
        // Credentials are collected from environment, config, profile or instance metadata
        Credentials::default()?,
    )?;

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
