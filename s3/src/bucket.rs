use serde_xml_rs as serde_xml;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::mem;
use tokio::runtime::Runtime;

use crate::command::Command;
use crate::request::{Headers, Query, Request};
use crate::serde_types::{BucketLocationResult, ListBucketResult, Tagging};
use crate::{Result, S3Error};
use awscreds::Credentials;
use awsregion::Region;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// # Example
/// ```
/// # // Fake  credentials so we don't access user's real credentials in tests
/// # use std::env;
/// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
/// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// use s3::bucket::Bucket;
/// use awscreds::Credentials;
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::default_blocking().unwrap();
///
/// let bucket = Bucket::new(bucket_name, region, credentials);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    pub name: String,
    pub region: Region,
    pub credentials: Credentials,
    pub extra_headers: Headers,
    pub extra_query: Query,
    path_style: bool
}

fn validate_expiry(expiry_secs: u32) -> Result<()> {
    if 604800 < expiry_secs {
        return Err(S3Error::from(format!("Max expiration for presigned URLs is one week, or 604.800 seconds, got {} instead", expiry_secs).as_ref()));
    }
    Ok(())
}

impl Bucket {
    /// Get a presigned url for getting object on a given path
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let url = bucket.presign_get("/test.file", 86400).unwrap();
    /// println!("Presigned url: {}", url);
    /// ```
    pub fn presign_get<S: AsRef<str>>(&self, path: S, expiry_secs: u32) -> Result<String> {
        validate_expiry(expiry_secs)?;
        let request = Request::new(self, path.as_ref(), Command::PresignGet { expiry_secs });
        Ok(request.presigned()?)
    }
    /// Get a presigned url for putting object to a given path
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let url = bucket.presign_put("/test.file", 86400).unwrap();
    /// println!("Presigned url: {}", url);
    /// ```
    pub fn presign_put<S: AsRef<str>>(&self, path: S, expiry_secs: u32) -> Result<String> {
        validate_expiry(expiry_secs)?;
        let request = Request::new(self, path.as_ref(), Command::PresignPut { expiry_secs });
        Ok(request.presigned()?)
    }
    /// Instantiate a new `Bucket`.
    ///
    /// # Example
    /// ```
    /// # // Fake  credentials so we don't access user's real credentials in tests
    /// # use std::env;
    /// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    /// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    ///
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// ```
    pub fn new(name: &str, region: Region, credentials: Credentials) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: false
        })
    }

    pub fn new_public(name: &str, region: Region) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials: Credentials::anonymous()?,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: false
        })
    }

    pub fn new_with_path_style(name: &str, region: Region, credentials: Credentials) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: true
        })
    }

    pub fn new_public_with_path_style(name: &str, region: Region) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials: Credentials::anonymous()?,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: true
        })
    }

    /// Gets file from an S3 path, blocks.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (data, code) = bucket.get_object_blocking("/test.file").unwrap();
    /// println!("Code: {}\nData: {:?}", code, data);
    /// ```
    pub fn get_object_blocking<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.get_object(path))?)
    }

    /// Gets file from an S3 path, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (data, code) = bucket.get_object("/test.file").await?;
    ///     println!("Code: {}", code);
    ///     println!("{:?}", data);
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::GetObject;
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.response_data_future().await?)
    }

    /// Stream file from S3 path to a local file, generic over T: Write, blocks.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use std::fs::File;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    /// let code = bucket.get_object_stream_blocking("/test.file", &mut output_file).unwrap();
    /// println!("Code: {}", code);
    /// ```
    pub fn get_object_stream_blocking<T: Write>(&self, path: &str, writer: &mut T) -> Result<u16> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.get_object_stream(path, writer))?)
    }

    /// Stream file from S3 path to a local file, generic over T: Write, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    ///     let status_code = bucket.get_object_stream("/test.file", &mut output_file).await?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_object_stream<T: Write, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16> {
        let command = Command::GetObject;
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.response_data_to_writer_future(writer).await?)
    }

    /// Stream file from S3 path to a local file, generic over T: Write, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    ///     let status_code = bucket.get_object_stream("/test.file", &mut output_file).await?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn tokio_get_object_stream<T: AsyncWriteExt + Unpin, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16> {
        let command = Command::GetObject;
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.tokio_response_data_to_writer_future(writer).await?)
    }

    /// Stream file from local path to s3, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut file = File::open("foo.txt")?;  
    ///
    ///     let status_code = bucket.put_object_stream(&mut file, "/test_file").await?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn put_object_stream<R: Read, S: AsRef<str>>(
        &self,
        reader: &mut R,
        s3_path: S,
    ) -> Result<u16> {
        let mut bytes = Vec::new();
        let read_n = reader.read(&mut bytes)?;
        debug!("Read {} bytes from reader", read_n);
        let command = Command::PutObject {
            content: &bytes[..],
            content_type: "application/octet-stream",
        };
        let request = Request::new(self, s3_path.as_ref(), command);
        Ok(request.response_data_future().await?.1)
    }

    /// Stream file from local path to s3 using tokio::io, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut file = File::open("foo.txt")?;  
    ///
    ///     let status_code = bucket.put_object_stream(&mut file, "/test_file").await?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn tokio_put_object_stream<R: AsyncReadExt + Unpin, S: AsRef<str>>(
        &self,
        reader: &mut R,
        s3_path: S,
    ) -> Result<u16> {
        let mut bytes = Vec::new();
        reader.read(&mut bytes).await?;
        let command = Command::PutObject {
            content: &bytes[..],
            content_type: "application/octet-stream",
        };
        let request = Request::new(self, s3_path.as_ref(), command);
        Ok(request.response_data_future().await?.1)
    }

    /// Stream file from local path to s3, blockIng.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default_blocking()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut file = File::open("foo.txt")?;
    ///
    ///     let status_code = bucket.put_object_stream_blocking(&mut file, "/test_file")?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    pub fn put_object_stream_blocking<R: Read, S: AsRef<str>>(
        &self,
        reader: &mut R,
        s3_path: S,
    ) -> Result<u16> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.put_object_stream(reader, s3_path))?)
    }

    //// Get bucket location from S3, async
    ////
    /// # Example
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "eu-central-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     println!("{}", bucket.location().await?.0);
    ///     Ok(())
    /// }
    /// ```
    pub async fn location(&self) -> Result<(Region, u16)> {
        let request = Request::new(self, "?location", Command::GetBucketLocation);
        let result = request.response_data_future().await?;
        let region_string = String::from_utf8_lossy(&result.0);
        let region = match serde_xml::from_reader(region_string.as_bytes()) {
            Ok(r) => {
                let location_result: BucketLocationResult = r;
                location_result.region.parse()?
            }
            Err(e) => {
                if result.1 == 200 {
                    Region::Custom {
                        region: "Custom".to_string(),
                        endpoint: "".to_string(),
                    }
                } else {
                    Region::Custom {
                        region: format!("Error encountered : {}", e),
                        endpoint: "".to_string(),
                    }
                }
            }
        };
        Ok((region, result.1))
    }

    //// Get bucket location from S3, async
    ////
    /// # Example
    /// ```rust,no_run
    /// # // Fake  credentials so we don't access user's real credentials in tests
    /// # use std::env;
    /// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    /// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "eu-central-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    ///
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// println!("{}", bucket.location_blocking().unwrap().0)
    /// ```
    pub fn location_blocking(&self) -> Result<(Region, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.location())?)
    }

    /// Delete file from an S3 path, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.delete_object("/test.file").await?;
    ///     assert_eq!(204, code);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObject;
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.response_data_future().await?)
    }

    /// Delete file from an S3 path, blocks .
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.delete_object_blocking("/test.file").unwrap();
    /// assert_eq!(204, code);
    /// ```
    pub fn delete_object_blocking<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.delete_object(path))?)
    }

    /// Put into an S3 bucket, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let aws_access = &"access_key";
    ///     let aws_secret = &"secret_key";
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse().unwrap();
    ///     let credentials = Credentials::default_blocking().unwrap();
    ///     let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    ///     let content = "I want to go to S3".as_bytes();
    ///     let (_, code) = bucket.put_object("/test.file", content, "text/plain").await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn put_object<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16)> {
        let command = Command::PutObject {
            content,
            content_type,
        };
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.response_data_future().await?)
    }

    /// Put into an S3 bucket, blocks.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let content = "I want to go to S3".as_bytes();
    /// let (_, code) = bucket.put_object_blocking("/test.file", content, "text/plain").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put_object_blocking<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.put_object(path, content, content_type))?)
    }

    fn _tags_xml<S: AsRef<str>>(&self, tags: &[(S, S)]) -> String {
        let mut s = String::new();
        let content = tags
            .iter()
            .map(|&(ref name, ref value)| {
                format!(
                    "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                    name.as_ref(),
                    value.as_ref()
                )
            })
            .fold(String::new(), |mut a, b| {
                a.push_str(b.as_str());
                a
            });
        s.push_str("<Tagging><TagSet>");
        s.push_str(&content);
        s.push_str("</TagSet></Tagging>");
        s
    }

    /// Tag an S3 object, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let aws_access = &"access_key";
    ///     let aws_secret = &"secret_key";
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn put_object_tagging<S: AsRef<str>>(
        &self,
        path: &str,
        tags: &[(S, S)],
    ) -> Result<(Vec<u8>, u16)> {
        let content = self._tags_xml(&tags);
        let command = Command::PutObjectTagging { tags: &content };
        let request = Request::new(self, path, command);
        Ok(request.response_data_future().await?)
    }

    /// Tag an S3 object, blocks.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.put_object_tagging_blocking("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put_object_tagging_blocking<S: AsRef<str>>(
        &self,
        path: &str,
        tags: &[(S, S)],
    ) -> Result<(Vec<u8>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.put_object_tagging(path, tags))?)
    }

    /// Delete tags from an S3 object, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let aws_access = &"access_key";
    ///     let aws_secret = &"secret_key";
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.delete_object_tagging("/test.file").await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete_object_tagging<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObjectTagging;
        let request = Request::new(self, path.as_ref(), command);
        Ok(request.response_data_future().await?)
    }

    /// Delete tags from an S3 object, blocks.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.delete_object_tagging_blocking("/test.file").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn delete_object_tagging_blocking<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.delete_object_tagging(path))?)
    }

    /// Retrieve an S3 object list of tags, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let aws_access = &"access_key";
    ///     let aws_secret = &"secret_key";
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (tags, code) = bucket.get_object_tagging("/test.file").await?;
    ///     if code == 200 {
    ///         for tag in tags.expect("No tags found").tag_set {
    ///             println!("{}={}", tag.key(), tag.value());
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_object_tagging<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<(Option<Tagging>, u16)> {
        let command = Command::GetObjectTagging {};
        let request = Request::new(self, path.as_ref(), command);
        let result = request.response_data_future().await?;

        let tagging = if result.1 == 200 {
            let result_string = String::from_utf8_lossy(&result.0);
            println!("{}", result_string);
            Some(serde_xml::from_reader(result_string.as_bytes())?)
        } else {
            None
        };

        Ok((tagging, result.1))
    }

    /// Retrieve an S3 object list of tags, blocks.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (tags, code) = bucket.get_object_tagging_blocking("/test.file").unwrap();
    /// if code == 200 {
    ///     for tag in tags.expect("No tags found").tag_set {
    ///         println!("{}={}", tag.key(), tag.value());
    ///     }
    /// }
    /// ```
    pub fn get_object_tagging_blocking<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<(Option<Tagging>, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.get_object_tagging(path))?)
    }

    pub fn list_page_blocking(
        &self,
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
    ) -> Result<(ListBucketResult, u16)> {
        let mut rt = Runtime::new()?;
        Ok(rt.block_on(self.list_page(prefix, delimiter, continuation_token))?)
    }

    pub async fn list_page(
        &self,
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
    ) -> Result<(ListBucketResult, u16)> {
        let command = Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
        };
        let request = Request::new(self, "/", command);
        let (response, status_code) = request.response_data_future().await?;
        match serde_xml::from_reader(response.as_slice()) {
            Ok(list_bucket_result) => Ok((list_bucket_result, status_code)),
            Err(_) => {
                let mut err = S3Error::from("Could not deserialize result");
                err.data = Some(String::from_utf8_lossy(response.as_slice()).to_string());
                Err(err)
            }
        }
    }

    /// List the contents of an S3 bucket, blocks.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::str;
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default_blocking().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let results = bucket.list_blocking("/".to_string(), Some("/".to_string())).unwrap();
    /// for (list, code) in results {
    ///     assert_eq!(200, code);
    ///     println!("{:?}", list);
    /// }
    /// ```
    pub fn list_blocking(
        &self,
        prefix: String,
        delimiter: Option<String>,
    ) -> Result<Vec<(ListBucketResult, u16)>> {
        let mut results = Vec::new();
        let mut result = self.list_page_blocking(prefix.clone(), delimiter.clone(), None)?;
        loop {
            results.push(result.clone());
            if !result.0.is_truncated {
                break;
            }
            match result.0.next_continuation_token {
                Some(token) => {
                    result =
                        self.list_page_blocking(prefix.clone(), delimiter.clone(), Some(token))?
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// List the contents of an S3 bucket, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// extern crate futures;
    /// use std::str;
    /// use s3::bucket::Bucket;
    /// use awscreds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let aws_access = &"access_key";
    ///     let aws_secret = &"secret_key";
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default().await?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let results = bucket.list("/".to_string(), Some("/".to_string())).await?;
    ///     println!("{:?}", results);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn list(
        &self,
        prefix: String,
        delimiter: Option<String>,
    ) -> Result<Vec<ListBucketResult>> {
        let the_bucket = self.to_owned();
        let mut results = Vec::new();
        let mut continuation_token = None;

        loop {
            let (list_bucket_result, _) = the_bucket
                .list_page(prefix.clone(), delimiter.clone(), continuation_token)
                .await?;
            continuation_token = list_bucket_result.next_continuation_token.clone();
            results.push(list_bucket_result);
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(results)
    }

    /// Get a reference to the name of the S3 bucket.
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    // Get a reference to the hostname of the S3 API endpoint.
    pub fn host(&self) -> String {
        if self.path_style {
            self.path_style_host()
        } else {
            self.subdomain_style_host()
        }
    }

    pub fn url(&self) -> String {
        if self.path_style {
            format!(
                "{}://{}/{}",
                self.scheme(),
                self.path_style_host(),
                self.name()
            )
        } else {
            format!("{}://{}", self.scheme(), self.subdomain_style_host())
        }
    }

    /// Get a paths-style reference to the hostname of the S3 API endpoint.
    pub fn path_style_host(&self) -> String {
        self.region.host()
    }

    pub fn subdomain_style_host(&self) -> String {
        format!("{}.{}", self.name, self.region.host())
    }

    // pub fn self_host(&self) -> String {
    //     format!("{}.{}", self.name, self.region.host())
    // }

    pub fn scheme(&self) -> String {
        self.region.scheme()
    }

    /// Get the region this object will connect to.
    pub fn region(&self) -> Region {
        self.region.clone()
    }

    /// Get a reference to the AWS access key.
    pub fn access_key(&self) -> Option<String> {
        if let Some(access_key) = self.credentials.access_key.clone() {
            Some(access_key.replace('\n', ""))
        } else {
            None
        }
    }

    /// Get a reference to the AWS secret key.
    pub fn secret_key(&self) -> Option<String> {
        if let Some(secret_key) = self.credentials.secret_key.clone() {
            Some(secret_key.replace('\n', ""))
        } else {
            None
        }
    }

    /// Get a reference to the AWS security token.
    pub fn security_token(&self) -> Option<&str> {
        self.credentials
            .security_token
            .as_ref()
            .map(std::string::String::as_str)
    }

    /// Get a reference to the AWS session token.
    pub fn session_token(&self) -> Option<&str> {
        self.credentials
            .session_token
            .as_ref()
            .map(std::string::String::as_str)
    }

    /// Get a reference to the full [`Credentials`](struct.Credentials.html)
    /// object used by this `Bucket`.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Change the credentials used by the Bucket, returning the existing
    /// credentials.
    pub fn set_credentials(&mut self, credentials: Credentials) -> Credentials {
        mem::replace(&mut self.credentials, credentials)
    }

    /// Add an extra header to send with requests to S3.
    ///
    /// Add an extra header to send with requests. Note that the library
    /// already sets a number of headers - headers set with this method will be
    /// overridden by the library headers:
    ///   * Host
    ///   * Content-Type
    ///   * Date
    ///   * Content-Length
    ///   * Authorization
    ///   * X-Amz-Content-Sha256
    ///   * X-Amz-Date
    pub fn add_header(&mut self, key: &str, value: &str) {
        self.extra_headers.insert(key.into(), value.into());
    }

    /// Get a reference to the extra headers to be passed to the S3 API.
    pub fn extra_headers(&self) -> &Headers {
        &self.extra_headers
    }

    /// Get a mutable reference to the extra headers to be passed to the S3
    /// API.
    pub fn extra_headers_mut(&mut self) -> &mut Headers {
        &mut self.extra_headers
    }

    /// Add an extra query pair to the URL used for S3 API access.
    pub fn add_query(&mut self, key: &str, value: &str) {
        self.extra_query.insert(key.into(), value.into());
    }

    /// Get a reference to the extra query pairs to be passed to the S3 API.
    pub fn extra_query(&self) -> &Query {
        &self.extra_query
    }

    /// Get a mutable reference to the extra query pairs to be passed to the S3
    /// API.
    pub fn extra_query_mut(&mut self) -> &mut Query {
        &mut self.extra_query
    }
}
