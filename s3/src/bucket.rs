use serde_xml_rs as serde_xml;
use std::collections::HashMap;
use std::mem;

use crate::bucket_ops::{BucketConfiguration, CreateBucketResponse};
use crate::command::Command;
use crate::creds::Credentials;
use crate::region::Region;

pub type Headers = HashMap<String, String>;
pub type Query = HashMap<String, String>;

#[cfg(feature = "async")]
use crate::request::Reqwest as RequestImpl;
#[cfg(feature = "async")]
// use tokio::io::AsyncWrite as TokioAsyncWrite;
#[cfg(feature = "async")]
use async_std::fs::File;
#[cfg(feature = "async")]
use async_std::path::Path;
#[cfg(feature = "async")]
use futures::io::AsyncRead;

#[cfg(feature = "sync")]
use crate::blocking::AttoRequest as RequestImpl;
#[cfg(feature = "sync")]
use std::fs::File;
#[cfg(feature = "sync")]
use std::io::Read;
#[cfg(feature = "sync")]
use std::path::Path;

use crate::request_trait::Request;
use crate::serde_types::{
    BucketLocationResult, CompleteMultipartUploadData, HeadObjectResult,
    InitiateMultipartUploadResponse, ListBucketResult, Part, Tagging,
};
use crate::{Result, S3Error};

pub const CHUNK_SIZE: usize = 8_388_608; // 8 Mebibytes, min is 5 (5_242_880);

/// # Example
/// ```
/// # // Fake  credentials so we don't access user's real credentials in tests
/// # use std::env;
/// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
/// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// use s3::bucket::Bucket;
/// use s3::creds::Credentials;
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::default().unwrap();
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
    path_style: bool,
}

fn validate_expiry(expiry_secs: u32) -> Result<()> {
    if 604800 < expiry_secs {
        return Err(S3Error::from(
            format!(
                "Max expiration for presigned URLs is one week, or 604.800 seconds, got {} instead",
                expiry_secs
            )
            .as_ref(),
        ));
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
    /// use s3::creds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let url = bucket.presign_get("/test.file", 86400).unwrap();
    /// println!("Presigned url: {}", url);
    /// ```
    pub fn presign_get<S: AsRef<str>>(&self, path: S, expiry_secs: u32) -> Result<String> {
        validate_expiry(expiry_secs)?;
        let request = RequestImpl::new(self, path.as_ref(), Command::PresignGet { expiry_secs });
        Ok(request.presigned()?)
    }

    /// Get a presigned url for putting object to a given path
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use std::collections::HashMap;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let mut custom_headers = HashMap::new();
    /// custom_headers.insert(
    ///    "custom_header".to_string(),
    ///    "custom_value".to_string(),
    /// );
    ///
    /// let url = bucket.presign_put("/test.file", 86400, Some(custom_headers)).unwrap();
    /// println!("Presigned url: {}", url);
    /// ```
    pub fn presign_put<S: AsRef<str>>(
        &self,
        path: S,
        expiry_secs: u32,
        custom_headers: Option<Headers>,
    ) -> Result<String> {
        validate_expiry(expiry_secs)?;
        let request = RequestImpl::new(
            self,
            path.as_ref(),
            Command::PresignPut {
                expiry_secs,
                custom_headers,
            },
        );
        Ok(request.presigned()?)
    }
    /// Create a new `Bucket` and instantiate it
    ///
    /// # Example
    /// ```rust,no_run
    /// use s3::{Bucket, BucketConfiguration};
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse().unwrap();
    ///     let credentials = Credentials::default().unwrap();
    ///     let config = BucketConfiguration::default();
    ///
    ///     let create_bucket_response = Bucket::create(bucket_name, region, credentials, config).await.unwrap();
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn create(
        name: &str,
        region: Region,
        credentials: Credentials,
        mut config: BucketConfiguration,
    ) -> Result<CreateBucketResponse> {
        config.set_region(region.clone());
        let command = Command::CreateBucket { config };
        let bucket = Bucket::new(name, region, credentials)?;
        let request = RequestImpl::new(&bucket, "", command);
        let (data, response_code) = request.response_data(false).await?;
        let response_text = std::str::from_utf8(&data)?;
        Ok(CreateBucketResponse {
            bucket,
            response_text: response_text.to_string(),
            response_code,
        })
    }

    /// Delete existing `Bucket`
    ///
    /// # Example
    /// ```rust,no_run
    /// use s3::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse().unwrap();
    ///     let credentials = Credentials::default().unwrap();
    ///     let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    ///     bucket.delete().await.unwrap();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete(&self) -> Result<u16> {
        let command = Command::DeleteBucket;
        let request = RequestImpl::new(self, "", command);
        let (_, response_code) = request.response_data(false).await?;
        Ok(response_code)
    }

    /// Instantiate an existing `Bucket`.
    ///
    /// # Example
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    /// // Fake  credentials so we don't access user's real credentials in tests
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
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
            path_style: false,
        })
    }

    pub fn new_public(name: &str, region: Region) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials: Credentials::anonymous()?,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: false,
        })
    }

    pub fn new_with_path_style(
        name: &str,
        region: Region,
        credentials: Credentials,
    ) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: true,
        })
    }

    pub fn new_public_with_path_style(name: &str, region: Region) -> Result<Bucket> {
        Ok(Bucket {
            name: name.into(),
            region,
            credentials: Credentials::anonymous()?,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
            path_style: true,
        })
    }

    /// Gets file from an S3 path, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (data, code) = bucket.get_object("/test.file").await?;
    ///     println!("Code: {}", code);
    ///     println!("{:?}", data);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Gets specified inclusive byte range of file from an S3 path, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     // The first thirty-two bytes of the object can be downloaded by specifying a range of 0 to 31.
    ///     let (data, code) = bucket.get_object_range("/test.file", 0, Some(31)).await?;
    ///     println!("Code: {}", code);
    ///     println!("{:?}", data);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object_range<S: AsRef<str>>(
        &self,
        path: S,
        start: u64,
        end: Option<u64>,
    ) -> Result<(Vec<u8>, u16)> {
        if let Some(end) = end {
            assert!(start < end);
        }

        let command = Command::GetObjectRange { start, end };
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Stream file from S3 path to a local file, generic over T: Write, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    /// use std::fs::File;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    ///     let status_code = bucket.get_object_stream("/test.file", &mut output_file).await?;
    ///     println!("Code: {}", status_code);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object_stream<T: std::io::Write, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data_to_writer(writer).await?)
    }

    // TODO doctest
    #[maybe_async::maybe_async]
    pub async fn put_object_stream(
        &self,
        path: impl AsRef<Path>,
        s3_path: impl AsRef<str>,
    ) -> Result<u16> {
        let mut file = File::open(path).await?;
        self._put_object_stream(&mut file, s3_path.as_ref()).await
    }

    #[maybe_async::async_impl]
    async fn _put_object_stream<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        s3_path: &str,
    ) -> Result<u16> {
        let command = Command::InitiateMultipartUpload;
        let path = format!("{}?uploads", s3_path);
        let request = RequestImpl::new(self, &path, command);
        let (data, code) = request.response_data(false).await?;
        let msg: InitiateMultipartUploadResponse =
            serde_xml::from_str(std::str::from_utf8(data.as_slice())?)?;

        let mut part_number: u32 = 0;
        let mut etags = Vec::new();
        loop {
            let chunk = crate::utils::read_chunk(reader).await?;

            if chunk.len() < CHUNK_SIZE {
                if part_number == 0 {
                    // Files is not big enough for multipart upload, going with regular put_object
                    let abort = Command::AbortMultipartUpload {
                        upload_id: &msg.upload_id,
                    };
                    let abort_path = format!("{}?uploadId={}", msg.key, &msg.upload_id);
                    let abort_request = RequestImpl::new(self, &abort_path, abort);
                    let (_, _code) = abort_request.response_data(false).await?;
                    self.put_object(s3_path, chunk.as_slice()).await?;
                    break;
                } else {
                    part_number += 1;
                    let command = Command::PutObject {
                        // part_number,
                        content: &chunk,
                        content_type: "application/octet-stream", // upload_id: &msg.upload_id,
                    };
                    let path = format!(
                        "{}?partNumber={}&uploadId={}",
                        msg.key, part_number, &msg.upload_id
                    );
                    let request = RequestImpl::new(self, &path, command);
                    let (data, _code) = request.response_data(true).await?;
                    let etag = std::str::from_utf8(data.as_slice())?;
                    etags.push(etag.to_string());
                    let inner_data = etags
                        .clone()
                        .into_iter()
                        .enumerate()
                        .map(|(i, x)| Part {
                            etag: x,
                            part_number: i as u32 + 1,
                        })
                        .collect::<Vec<Part>>();
                    let data = CompleteMultipartUploadData { parts: inner_data };
                    let complete = Command::CompleteMultipartUpload {
                        upload_id: &msg.upload_id,
                        data,
                    };
                    let complete_path = format!("{}?uploadId={}", msg.key, &msg.upload_id);
                    let complete_request = RequestImpl::new(self, &complete_path, complete);
                    let (_data, _code) = complete_request.response_data(false).await?;
                    // let response = std::str::from_utf8(data.as_slice())?;
                    break;
                }
            } else {
                part_number += 1;
                let command = Command::PutObject {
                    // part_number,
                    content: &chunk,
                    content_type: "application/octet-stream", // upload_id: &msg.upload_id,
                };
                let path = format!(
                    "{}?partNumber={}&uploadId={}",
                    msg.key, part_number, &msg.upload_id
                );
                let request = RequestImpl::new(self, &path, command);
                let (data, _code) = request.response_data(true).await?;
                let etag = std::str::from_utf8(data.as_slice())?;
                etags.push(etag.to_string());
            }
        }
        Ok(code)
    }

    #[maybe_async::sync_impl]
    fn _put_object_stream<R: Read>(&self, reader: &mut R, s3_path: &str) -> Result<u16> {
        let command = Command::InitiateMultipartUpload;
        let path = format!("{}?uploads", s3_path);
        let request = RequestImpl::new(self, &path, command);
        let (data, code) = request.response_data(false)?;
        let msg: InitiateMultipartUploadResponse =
            serde_xml::from_str(std::str::from_utf8(data.as_slice())?)?;

        let mut part_number: u32 = 0;
        let mut etags = Vec::new();
        loop {
            let chunk = crate::utils::read_chunk_blocking(reader)?;

            if chunk.len() < CHUNK_SIZE {
                if part_number == 0 {
                    // Files is not big enough for multipart upload, going with regular put_object
                    let abort = Command::AbortMultipartUpload {
                        upload_id: &msg.upload_id,
                    };
                    let abort_path = format!("{}?uploadId={}", msg.key, &msg.upload_id);
                    let abort_request = RequestImpl::new(self, &abort_path, abort);
                    let (_, _code) = abort_request.response_data(false)?;
                    self.put_object(s3_path, chunk.as_slice())?;
                    break;
                } else {
                    part_number += 1;
                    let command = Command::PutObject {
                        // part_number,
                        content: &chunk,
                        content_type: "application/octet-stream", // upload_id: &msg.upload_id,
                    };
                    let path = format!(
                        "{}?partNumber={}&uploadId={}",
                        msg.key, part_number, &msg.upload_id
                    );
                    let request = RequestImpl::new(self, &path, command);
                    let (data, _code) = request.response_data(true)?;
                    let etag = std::str::from_utf8(data.as_slice())?;
                    etags.push(etag.to_string());
                    let inner_data = etags
                        .into_iter()
                        .enumerate()
                        .map(|(i, x)| Part {
                            etag: x,
                            part_number: i as u32 + 1,
                        })
                        .collect::<Vec<Part>>();
                    let data = CompleteMultipartUploadData { parts: inner_data };
                    let complete = Command::CompleteMultipartUpload {
                        upload_id: &msg.upload_id,
                        data,
                    };
                    let complete_path = format!("{}?uploadId={}", msg.key, &msg.upload_id);
                    let complete_request = RequestImpl::new(self, &complete_path, complete);
                    let (_data, _code) = complete_request.response_data(false)?;
                    // let response = std::str::from_utf8(data.as_slice())?;
                    break;
                }
            } else {
                part_number += 1;
                let command = Command::PutObject {
                    // part_number,
                    content: &chunk,
                    content_type: "application/octet-stream", // upload_id: &msg.upload_id,
                };
                let path = format!(
                    "{}?partNumber={}&uploadId={}",
                    msg.key, part_number, &msg.upload_id
                );
                let request = RequestImpl::new(self, &path, command);
                let (data, _code) = request.response_data(true)?;
                let etag = std::str::from_utf8(data.as_slice())?;
                etags.push(etag.to_string());
            }
        }
        Ok(code)
    }

    //// Get bucket location from S3, async
    ////
    /// # Example
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = "rust-s3-test";
    ///     let region = "eu-central-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     println!("{}", bucket.location().await?.0);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn location(&self) -> Result<(Region, u16)> {
        let request = RequestImpl::new(self, "?location", Command::GetBucketLocation);
        let result = request.response_data(false).await?;
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

    /// Delete file from an S3 path, async.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.delete_object("/test.file").await?;
    ///     assert_eq!(204, code);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Head object from S3, async.
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::S3Error;
    /// #[tokio::main]
    /// async fn main() -> Result<(), S3Error> {
    ///     let bucket_name = &"rust-s3-test";
    ///     let region = "us-east-1".parse()?;
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///     let (head_object_result, code) = bucket.head_object("/test.png").await.unwrap();
    ///     assert_eq!(head_object_result.content_type.unwrap() , "image/png".to_owned());
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn head_object<S: AsRef<str>>(&self, path: S) -> Result<(HeadObjectResult, u16)> {
        let command = Command::HeadObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        let (headers, status) = request.response_header().await?;
        let header_object = HeadObjectResult::from(&headers);
        Ok((header_object, status))
    }

    /// Put into an S3 bucket, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default().unwrap();
    ///     let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    ///     let content = "I want to go to S3".as_bytes();
    ///     let (_, code) = bucket.put_object_with_content_type("/test.file", content, "text/plain").await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_with_content_type<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16)> {
        let command = Command::PutObject {
            content,
            content_type,
        };
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(true).await?)
    }

    /// Put into an S3 bucket, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default().unwrap();
    ///     let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    ///     let content = "I want to go to S3".as_bytes();
    ///     let (_, code) = bucket.put_object("/test.file", content).await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
    ) -> Result<(Vec<u8>, u16)> {
        self.put_object_with_content_type(path, content, "application/octet-stream")
            .await
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
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_tagging<S: AsRef<str>>(
        &self,
        path: &str,
        tags: &[(S, S)],
    ) -> Result<(Vec<u8>, u16)> {
        let content = self._tags_xml(&tags);
        let command = Command::PutObjectTagging { tags: &content };
        let request = RequestImpl::new(self, path, command);
        Ok(request.response_data(false).await?)
    }

    /// Delete tags from an S3 object, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let (_, code) = bucket.delete_object_tagging("/test.file").await?;
    ///     assert_eq!(201, code);
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object_tagging<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObjectTagging;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Retrieve an S3 object list of tags, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default()?;
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
    #[maybe_async::maybe_async]
    pub async fn get_object_tagging<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<(Option<Tagging>, u16)> {
        let command = Command::GetObjectTagging {};
        let request = RequestImpl::new(self, path.as_ref(), command);
        let result = request.response_data(false).await?;

        let tagging = if result.1 == 200 {
            let result_string = String::from_utf8_lossy(&result.0);
            println!("{}", result_string);
            Some(serde_xml::from_reader(result_string.as_bytes())?)
        } else {
            None
        };

        Ok((tagging, result.1))
    }

    #[maybe_async::maybe_async]
    pub async fn list_page(
        &self,
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<usize>,
    ) -> Result<(ListBucketResult, u16)> {
        let command = Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
            start_after,
            max_keys,
        };
        let request = RequestImpl::new(self, "/", command);
        let (response, status_code) = request.response_data(false).await?;
        match serde_xml::from_reader(response.as_slice()) {
            Ok(list_bucket_result) => Ok((list_bucket_result, status_code)),
            Err(_) => {
                let mut err = S3Error::from("Could not deserialize result");
                err.data = Some(String::from_utf8_lossy(response.as_slice()).to_string());
                Err(err)
            }
        }
    }

    /// List the contents of an S3 bucket, async.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// extern crate futures;
    /// use std::str;
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
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
    ///     let credentials = Credentials::default()?;
    ///     let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    ///     let results = bucket.list("/".to_string(), Some("/".to_string())).await?;
    ///     println!("{:?}", results);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[maybe_async::maybe_async]
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
                .list_page(
                    prefix.clone(),
                    delimiter.clone(),
                    continuation_token,
                    None,
                    None,
                )
                .await?;
            continuation_token = list_bucket_result.next_continuation_token.clone();
            results.push(list_bucket_result);
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(results)
    }

    /// Get path_style field of the Bucket struct
    pub fn is_path_style(&self) -> bool {
        self.path_style
    }

    // Get negated path_style field of the Bucket struct
    pub fn is_subdomain_style(&self) -> bool {
        !self.path_style
    }

    /// Configure bucket to use path-style urls and headers
    pub fn set_path_style(&mut self) {
        self.path_style = true;
    }

    /// Configure bucket to use subdomain style urls and headers [default]
    pub fn set_subdomain_style(&mut self) {
        self.path_style = false;
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

#[cfg(test)]
mod test {

    use crate::creds::Credentials;
    use crate::region::Region;
    use crate::Bucket;
    use crate::BucketConfiguration;
    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;

    fn test_aws_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("EU_AWS_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("EU_AWS_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_gc_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("GC_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("GC_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_wasabi_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("WASABI_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("WASABI_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_aws_bucket() -> Bucket {
        Bucket::new(
            "rust-s3-test",
            "eu-central-1".parse().unwrap(),
            test_aws_credentials(),
        )
        .unwrap()
    }

    fn test_wasabi_bucket() -> Bucket {
        Bucket::new(
            "rust-s3",
            "wa-eu-central-1".parse().unwrap(),
            test_wasabi_credentials(),
        )
        .unwrap()
    }

    fn test_gc_bucket() -> Bucket {
        Bucket::new(
            "rust-s3",
            Region::Custom {
                region: "us-east1".to_owned(),
                endpoint: "https://storage.googleapis.com".to_owned(),
            },
            test_gc_credentials(),
        )
        .unwrap()
    }

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn streaming_test_put_get_delete_big_object() {
        let path = "stream_test_big";
        std::fs::remove_file(path).unwrap_or_else(|_| {});
        let bucket = test_aws_bucket();
        let test: Vec<u8> = object(10_000_000);

        let mut file = File::create(path).unwrap();
        file.write_all(&test).unwrap();

        let code = bucket
            .put_object_stream(path, "/stream_test_big.file")
            .await
            .unwrap();
        assert_eq!(code, 200);
        let mut writer = Vec::new();
        let code = bucket
            .get_object_stream("/stream_test_big.file", &mut writer)
            .await
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(test, writer);
        let (_, code) = bucket.delete_object("/stream_test_big.file").await.unwrap();
        assert_eq!(code, 204);
        std::fs::remove_file(path).unwrap_or_else(|_| {});
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn test_put_head_get_delete_object() {
        let s3_path = "/test.file";
        let bucket = test_aws_bucket();
        let test: Vec<u8> = object(3072);

        let (_data, code) = bucket.put_object(s3_path, &test).await.unwrap();
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(code, 200);
        let (data, code) = bucket.get_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test, data);

        let (data, code) = bucket
            .get_object_range(s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(code, 206);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test[100..1001].to_vec(), data);

        let (head_object_result, code) = bucket.head_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);
        let (_, code) = bucket.delete_object(s3_path).await.unwrap();
        assert_eq!(code, 204);
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn gc_test_put_head_get_delete_object() {
        let s3_path = "/test.file";
        let bucket = test_gc_bucket();
        let test: Vec<u8> = object(3072);

        let (_data, code) = bucket.put_object(s3_path, &test).await.unwrap();
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(code, 200);
        let (data, code) = bucket.get_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test, data);

        let (data, code) = bucket
            .get_object_range(s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(code, 206);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test[100..1001].to_vec(), data);

        let (head_object_result, code) = bucket.head_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);
        let (_, code) = bucket.delete_object(s3_path).await.unwrap();
        assert_eq!(code, 204);
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn wasabi_test_put_head_get_delete_object() {
        let s3_path = "/test.file";
        let bucket = test_wasabi_bucket();
        let test: Vec<u8> = object(3072);

        let (_data, code) = bucket.put_object(s3_path, &test).await.unwrap();
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(code, 200);
        let (data, code) = bucket.get_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test, data);

        let (data, code) = bucket
            .get_object_range(s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(code, 206);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test[100..1001].to_vec(), data);

        let (head_object_result, code) = bucket.head_object(s3_path).await.unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);
        let (_, code) = bucket.delete_object(s3_path).await.unwrap();
        assert_eq!(code, 204);
    }

    #[cfg(feature = "sync")]
    use attohttpc::header::{HeaderMap, HeaderName, HeaderValue};
    #[cfg(feature = "async")]
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    #[ignore]
    fn test_presign_put() {
        let s3_path = "/test/test.file";
        let bucket = test_aws_bucket();

        let mut custom_headers = HashMap::new();
        custom_headers.insert("custom_header".to_string(), "custom_value".to_string());

        let url = bucket
            .presign_put(s3_path, 86400, Some(custom_headers))
            .unwrap();

        // assert_eq!(url, "");

        assert!(url.contains("host%3Bcustom_header"));
        assert!(url.contains("/test%2Ftest.file"))
    }

    #[test]
    #[ignore]
    fn test_presign_get() {
        let s3_path = "/test/test.file";
        let bucket = test_aws_bucket();

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(
            HeaderName::from_static("custom_header"),
            HeaderValue::from_str("custom_value").unwrap(),
        );

        let url = bucket.presign_get(s3_path, 86400).unwrap();
        assert!(url.contains("/test%2Ftest.file?"))
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[ignore]
    async fn test_bucket_create_delete_default_region() {
        let config = BucketConfiguration::default();
        let response = Bucket::create(
            &uuid::Uuid::new_v4().to_string(),
            "us-east-1".parse().unwrap(),
            test_aws_credentials(),
            config,
        )
        .await
        .unwrap();

        assert_eq!(&response.response_text, "");

        assert_eq!(response.response_code, 200);

        let response_code = response.bucket.delete().await.unwrap();
        assert!(response_code < 300);
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn test_bucket_create_delete_non_default_region() {
        let config = BucketConfiguration::default();
        let response = Bucket::create(
            &uuid::Uuid::new_v4().to_string(),
            "eu-central-1".parse().unwrap(),
            test_aws_credentials(),
            config,
        )
        .await
        .unwrap();

        assert_eq!(&response.response_text, "");

        assert_eq!(response.response_code, 200);

        let response_code = response.bucket.delete().await.unwrap();
        assert!(response_code < 300);
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "async"), tokio::test)
    )]
    async fn test_bucket_create_delete_non_default_region_public() {
        let config = BucketConfiguration::public();
        let response = Bucket::create(
            &uuid::Uuid::new_v4().to_string(),
            "eu-central-1".parse().unwrap(),
            test_aws_credentials(),
            config,
        )
        .await
        .unwrap();

        assert_eq!(&response.response_text, "");

        assert_eq!(response.response_code, 200);

        let response_code = response.bucket.delete().await.unwrap();
        assert!(response_code < 300);
    }
}
