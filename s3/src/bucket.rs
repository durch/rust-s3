#[cfg(feature = "blocking")]
use block_on_proc::block_on;
use serde_xml_rs as serde_xml;
use std::collections::HashMap;
use std::mem;

use crate::bucket_ops::{BucketConfiguration, CreateBucketResponse};
use crate::command::Command;
use crate::creds::Credentials;
use crate::region::Region;

pub type Headers = HashMap<String, String>;
pub type Query = HashMap<String, String>;

#[cfg(feature = "with-tokio")]
use crate::request::Reqwest as RequestImpl;
#[cfg(feature = "with-async-std")]
use crate::surf_request::SurfRequest as RequestImpl;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use async_std::fs::File;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use async_std::path::Path;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
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
use anyhow::Result;
use anyhow::anyhow;

pub const CHUNK_SIZE: usize = 8_388_608; // 8 Mebibytes, min is 5 (5_242_880);

/// Instantiate an existing Bucket
///
/// # Example
///
/// ```no_run
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
        return Err(anyhow!(
                "Max expiration for presigned URLs is one week, or 604.800 seconds, got {} instead",
                expiry_secs
            )
        );
    }
    Ok(())
}

#[cfg_attr(all(feature = "with-tokio", feature = "blocking"), block_on("tokio"))]
#[cfg_attr(
    all(feature = "with-async-std", feature = "blocking"),
    block_on("async-std")
)]
impl Bucket {
    /// Get a presigned url for getting object on a given path
    ///
    /// # Example:
    ///
    /// ```no_run
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
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use std::collections::HashMap;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// // Add optional custom headers
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
    /// ```no_run
    /// use s3::{Bucket, BucketConfiguration};
    /// use s3::creds::Credentials;
    /// # use s3::region::Region;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let config = BucketConfiguration::default();
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let create_bucket_response = Bucket::create(bucket_name, region, credentials, config).await?;
    ///
    /// // `sync` fature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let create_bucket_response = Bucket::create(bucket_name, region, credentials, config)?;
    ///
    /// # let region: Region = "us-east-1".parse()?;
    /// # let credentials = Credentials::default()?;
    /// # let config = BucketConfiguration::default();
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let create_bucket_response = Bucket::create_blocking(bucket_name, region, credentials, config)?;
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn create(
        name: &str,
        region: Region,
        credentials: Credentials,
        config: BucketConfiguration,
    ) -> Result<CreateBucketResponse> {
        let mut config = config;
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

    /// Create a new `Bucket` with path style and instantiate it
    ///
    /// ```no_run
    /// use s3::{Bucket, BucketConfiguration};
    /// use s3::creds::Credentials;
    /// # use s3::region::Region;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let config = BucketConfiguration::default();
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let create_bucket_response = Bucket::create_with_path_style(bucket_name, region, credentials, config).await?;
    ///
    /// // `sync` fature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let create_bucket_response = Bucket::create_with_path_style(bucket_name, region, credentials, config)?;
    ///
    /// # let region: Region = "us-east-1".parse()?;
    /// # let credentials = Credentials::default()?;
    /// # let config = BucketConfiguration::default();
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let create_bucket_response = Bucket::create_with_path_style_blocking(bucket_name, region, credentials, config)?;
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn create_with_path_style(
        name: &str,
        region: Region,
        credentials: Credentials,
        mut config: BucketConfiguration,
    ) -> Result<CreateBucketResponse> {
        config.set_region(region.clone());
        let command = Command::CreateBucket { config };
        let bucket = Bucket::new_with_path_style(name, region, credentials)?;
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
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// bucket.delete().await.unwrap();
    /// // `sync` fature will produce an identical method
    ///
    /// #[cfg(feature = "sync")]
    /// bucket.delete().unwrap();
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    ///
    /// #[cfg(feature = "blocking")]
    /// bucket.delete_blocking().unwrap();
    ///
    /// # Ok(())
    /// # }
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
    /// ```no_run
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

    /// Instantiate a public existing `Bucket`.
    ///
    /// # Example
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    ///
    /// let bucket = Bucket::new_public(bucket_name, region).unwrap();
    /// ```
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

    /// Instantiate an existing `Bucket` with path style addressing. Useful for compatibility with some storage APIs, like MinIO.
    ///
    /// # Example
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    ///
    /// let bucket = Bucket::new_with_path_style(bucket_name, region, credentials).unwrap();
    /// ```
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

    /// Instantiate a public existing `Bucket` with path style addressing. Useful for compatibility with some storage APIs, like MinIO.
    ///
    /// # Example
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    ///
    /// let bucket = Bucket::new_public_with_path_style(bucket_name, region).unwrap();
    /// ```
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

    /// Gets file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (data, code) = bucket.get_object("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (data, code) = bucket.get_object("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (data, code) = bucket.get_object_blocking("/test.file")?;
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Gets specified inclusive byte range of file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (data, code) = bucket.get_object_range("/test.file", 0, Some(31)).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (data, code) = bucket.get_object_range("/test.file", 0, Some(31))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (data, code) = bucket.get_object_range_blocking("/test.file", 0, Some(31))?;
    /// #
    /// # Ok(())
    /// # }
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

    /// Stream file from S3 path to a local file, generic over T: Write.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    /// use std::fs::File;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    /// let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let status_code = bucket.get_object_stream("/test.file", &mut output_file).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket.get_object_stream("/test.file", &mut output_file)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.get_object_stream_blocking("/test.file", &mut output_file)?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object_stream<T: std::io::Write + Send, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data_to_writer(writer).await?)
    }

    /// Stream file from local path to s3, generic over T: Write.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    /// use std::fs::File;
    /// use std::io::Write;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    /// let path = "path";
    /// let test: Vec<u8> = (0..1000).map(|_| 42).collect();
    /// let mut file = File::create(path)?;
    /// file.write_all(&test)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let status_code = bucket.put_object_stream(path, "/path").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket.put_object_stream(path, "/path")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.put_object_stream_blocking(path, "/path")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_stream(
        &self,
        path: impl AsRef<Path>,
        s3_path: impl AsRef<str>,
    ) -> Result<u16> {
        self._put_object_stream(&mut File::open(path).await?, s3_path.as_ref()).await
    }

    #[maybe_async::async_impl]
    async fn _put_object_stream<R: AsyncRead + Unpin + Send>(
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
            let chunk = crate::utils::read_chunk(reader)?;

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

    /// Get Bucket location.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (region, status_code) = bucket.location().await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (region, status_code) = bucket.location()?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (region, status_code) = bucket.location_blocking()?;
    /// #
    /// # Ok(())
    /// # }
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

    /// Delete file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.delete_object("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.delete_object("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.delete_object_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Head object from S3.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (head_object_result, code) = bucket.head_object("/test.png").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (head_object_result, code) = bucket.head_object("/test.png")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (head_object_result, code) = bucket.head_object_blocking("/test.png")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn head_object<S: AsRef<str>>(&self, path: S) -> Result<(HeadObjectResult, u16)> {
        let command = Command::HeadObject;
        let request = RequestImpl::new(self, path.as_ref(), command);
        let (headers, status) = request.response_header().await?;
        let header_object = HeadObjectResult::from(&headers);
        Ok((header_object, status))
    }

    /// Put into an S3 bucket, with explicit content-type.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    /// let content = "I want to go to S3".as_bytes();
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.put_object_with_content_type("/test.file", content, "text/plain").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.put_object_with_content_type("/test.file", content, "text/plain")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.put_object_with_content_type_blocking("/test.file", content, "text/plain")?;
    /// #
    /// # Ok(())
    /// # }
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

    /// Put into an S3 bucket.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    /// let content = "I want to go to S3".as_bytes();
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.put_object("/test.file", content).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.put_object("/test.file", content)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.put_object_blocking("/test.file", content)?;
    /// #
    /// # Ok(())
    /// # }
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

    /// Tag an S3 object.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")])?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.put_object_tagging_blocking("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")])?;
    /// #
    /// # Ok(())
    /// # }
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

    /// Delete tags from an S3 object.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.delete_object_tagging("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.delete_object_tagging("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.delete_object_tagging_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object_tagging<S: AsRef<str>>(&self, path: S) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObjectTagging;
        let request = RequestImpl::new(self, path.as_ref(), command);
        Ok(request.response_data(false).await?)
    }

    /// Retrieve an S3 object list of tags.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let (_, code) = bucket.get_object_tagging("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let (_, code) = bucket.get_object_tagging("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let (_, code) = bucket.get_object_tagging_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
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
        return serde_xml::from_reader(response.as_slice())
            .map(|list_bucket_result| (list_bucket_result, status_code))
            .map_err(|e|anyhow!("Could not deserialize result \n {}",e))
    }

    /// List the contents of an S3 bucket.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let results = bucket.list("/".to_string(), Some("/".to_string())).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let results = bucket.list("/".to_string(), Some("/".to_string()))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let results = bucket.list_blocking("/".to_string(), Some("/".to_string()))?;
    /// #
    /// # Ok(())
    /// # }
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
            .as_deref()
    }

    /// Get a reference to the AWS session token.
    pub fn session_token(&self) -> Option<&str> {
        self.credentials
            .session_token
            .as_deref()
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
    // use log::info;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

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
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn streaming_test_put_get_delete_big_object() {
        init();
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
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
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
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn test_put_head_get_delete_object_blocking() {
        let s3_path = "/test_blocking.file";
        let bucket = test_aws_bucket();
        let test: Vec<u8> = object(3072);

        let (_data, code) = bucket.put_object_blocking(s3_path, &test).unwrap();
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(code, 200);
        let (data, code) = bucket.get_object_blocking(s3_path).unwrap();
        assert_eq!(code, 200);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test, data);

        let (data, code) = bucket
            .get_object_range_blocking(s3_path, 100, Some(1000))
            .unwrap();
        assert_eq!(code, 206);
        // println!("{}", std::str::from_utf8(&data).unwrap());
        assert_eq!(test[100..1001].to_vec(), data);

        let (head_object_result, code) = bucket.head_object_blocking(s3_path).unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);
        let (_, code) = bucket.delete_object_blocking(s3_path).unwrap();
        assert_eq!(code, 204);
    }

    #[ignore]
    #[maybe_async::test(
        feature = "sync",
        async(
            all(
                not(any(feature = "sync", feature = "tokio-rustls-tls")),
                feature = "with-tokio"
            ),
            tokio::test
        ),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
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
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
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

        let url = bucket.presign_get(s3_path, 86400).unwrap();
        assert!(url.contains("/test%2Ftest.file?"))
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
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
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
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
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
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
