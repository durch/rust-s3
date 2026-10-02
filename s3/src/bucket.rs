//! # Rust S3 Bucket Operations
//!
//! This module provides functionality for interacting with S3 buckets and objects,
//! including creating, listing, uploading, downloading, and deleting objects. It supports
//! various features such as asynchronous and blocking operations, multipart uploads,
//! presigned URLs, and tagging objects.
//!
//! ## Features
//!
//! The module supports the following features:
//!
//! - **blocking**: Enables blocking (synchronous) operations using the `block_on` macro.
//! - **tags**: Adds support for managing S3 object tags.
//! - **with-tokio**: Enables asynchronous operations using the Tokio runtime.
//! - **with-async-std**: Enables asynchronous operations using the async-std runtime.
//! - **sync**: Enables synchronous (blocking) operations using standard Rust synchronization primitives.
//!
//! ## Constants
//!
//! - `CHUNK_SIZE`: Defines the chunk size for multipart uploads (8 MiB).
//! - `DEFAULT_REQUEST_TIMEOUT`: The default request timeout (60 seconds).
//!
//! ## Types
//!
//! - `Query`: A type alias for `HashMap<String, String>`, representing query parameters for requests.
//!
//! ## Structs
//!
//! - `Bucket`: Represents an S3 bucket, providing methods to interact with the bucket and its contents.
//! - `Tag`: Represents a key-value pair used for tagging S3 objects.
//!
//! ## Errors
//!
//! - `S3Error`: Represents various errors that can occur during S3 operations.

#[cfg(feature = "blocking")]
use block_on_proc::block_on;
#[cfg(feature = "tags")]
use minidom::Element;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::bucket_ops::{BucketConfiguration, CreateBucketResponse};
use crate::command::{Command, Multipart};
use crate::creds::Credentials;
use crate::region::Region;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use crate::request::ResponseDataStream;
#[cfg(feature = "with-tokio")]
use crate::request::tokio_backend::ClientOptions;
#[cfg(feature = "with-tokio")]
use crate::request::tokio_backend::client;
use crate::request::{Request as _, ResponseData};
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "with-tokio")]
use tokio::sync::{Mutex as AsyncMutex, RwLock};

#[cfg(feature = "with-async-std")]
use async_std::sync::{Mutex as AsyncMutex, RwLock};

#[cfg(feature = "sync")]
use std::sync::RwLock;

pub type Query = HashMap<String, String>;

#[cfg(feature = "with-async-std")]
use crate::request::async_std_backend::SurfRequest as RequestImpl;
#[cfg(feature = "with-tokio")]
use crate::request::tokio_backend::ReqwestRequest as RequestImpl;

#[cfg(feature = "with-async-std")]
use async_std::io::Write as AsyncWrite;
#[cfg(feature = "with-tokio")]
use tokio::io::AsyncWrite;

#[cfg(feature = "sync")]
use crate::request::blocking::AttoRequest as RequestImpl;
use std::io::Read;

#[cfg(feature = "with-tokio")]
use tokio::io::AsyncRead;

#[cfg(feature = "with-async-std")]
use async_std::io::Read as AsyncRead;

use crate::PostPolicy;
use crate::error::S3Error;
use crate::post_policy::PresignedPost;
use crate::serde_types::{
    BucketLifecycleConfiguration, BucketLocationResult, CompleteMultipartUploadData,
    CorsConfiguration, DeleteObjectsRequest, DeleteObjectsResult, GetObjectAttributesOutput,
    HeadObjectResult, InitiateMultipartUploadResponse, ListBucketResult,
    ListMultipartUploadsResult, ObjectIdentifier, Part,
};
#[allow(unused_imports)]
use crate::utils::{PutStreamResponse, error_from_response_data};

use http::HeaderMap;
use http::header::HeaderName;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use sysinfo::{MemoryRefreshKind, System};

fn validate_success_xml_response(
    response_data: ResponseData,
    expected_root: &str,
) -> Result<ResponseData, S3Error> {
    if !(200..300).contains(&response_data.status_code()) {
        return Ok(response_data);
    }

    let body = response_data.as_slice();
    let body_text = std::str::from_utf8(body)?;
    let mut reader = quick_xml::Reader::from_str(body_text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().check_comments = true;
    let mut depth = 0usize;
    let mut root = None;
    let mut valid = true;
    let mut declaration_seen = false;
    loop {
        use quick_xml::events::Event;
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                for attr in element.attributes() {
                    match attr {
                        Ok(attr)
                            if attr
                                .decoded_and_normalized_value(
                                    quick_xml::XmlVersion::Implicit1_0,
                                    reader.decoder(),
                                )
                                .is_ok() => {}
                        _ => {
                            valid = false;
                            break;
                        }
                    }
                }
                if depth == 0 {
                    if root.is_some() {
                        valid = false;
                        break;
                    }
                    root =
                        Some(String::from_utf8_lossy(element.local_name().as_ref()).into_owned());
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                for attr in element.attributes() {
                    match attr {
                        Ok(attr)
                            if attr
                                .decoded_and_normalized_value(
                                    quick_xml::XmlVersion::Implicit1_0,
                                    reader.decoder(),
                                )
                                .is_ok() => {}
                        _ => {
                            valid = false;
                            break;
                        }
                    }
                }
                if depth == 0 {
                    if root.is_some() {
                        valid = false;
                        break;
                    }
                    root =
                        Some(String::from_utf8_lossy(element.local_name().as_ref()).into_owned());
                }
            }
            Ok(Event::End(_)) => {
                if depth == 0 {
                    valid = false;
                    break;
                }
                depth -= 1;
            }
            Ok(Event::Text(text)) => {
                // quick-xml 0.38's xml_content() used XML 1.1 normalization.
                // Keep that behavior explicit across the 0.41 API change.
                if text.xml11_content().is_err()
                    || (depth == 0 && !text.as_ref().iter().all(u8::is_ascii_whitespace))
                {
                    valid = false;
                    break;
                }
            }
            Ok(Event::GeneralRef(reference)) => {
                let name = reference.as_ref();
                let predefined = matches!(name, b"amp" | b"lt" | b"gt" | b"apos" | b"quot");
                if depth == 0
                    || (!predefined && reference.resolve_char_ref().ok().flatten().is_none())
                {
                    valid = false;
                    break;
                }
            }
            Ok(Event::Eof) => break,
            Ok(Event::DocType(_)) => {
                valid = false;
                break;
            }
            Ok(Event::Decl(declaration)) if root.is_none() && !declaration_seen => {
                declaration_seen = true;
                if !matches!(declaration.version().as_deref(), Ok(b"1.0") | Ok(b"1.1")) {
                    valid = false;
                    break;
                }
            }
            Ok(Event::Decl(_)) => {
                valid = false;
                break;
            }
            Ok(Event::Comment(_) | Event::PI(_)) => {}
            Ok(Event::CData(_)) if depth > 0 => {}
            Ok(_) => {
                valid = false;
                break;
            }
            Err(_) => {
                valid = false;
                break;
            }
        }
    }

    if !valid || depth != 0 || root.is_none() {
        return Err(S3Error::SerdeXml(quick_xml::de::DeError::Custom(
            "invalid or incomplete XML response".to_owned(),
        )));
    }
    match root.as_deref() {
        Some("Error") => Err(error_from_response_data(response_data)?),
        Some(root) if root == expected_root => Ok(response_data),
        _ => Err(S3Error::HttpFailWithBody(
            response_data.status_code(),
            String::from_utf8_lossy(body).into_owned(),
        )),
    }
}

fn response_error_from_data(response_data: ResponseData) -> S3Error {
    match error_from_response_data(response_data) {
        Ok(error) | Err(error) => error,
    }
}

#[cfg(all(
    not(feature = "sync"),
    any(feature = "with-tokio", feature = "with-async-std")
))]
#[derive(Default)]
struct MultipartHeaderPlan {
    initiate: HeaderMap,
    part: HeaderMap,
    complete: HeaderMap,
    abort: HeaderMap,
}

#[cfg(all(
    not(feature = "sync"),
    any(feature = "with-tokio", feature = "with-async-std")
))]
impl MultipartHeaderPlan {
    fn from_headers(headers: Option<&HeaderMap>) -> Result<Self, S3Error> {
        let mut plan = Self::default();
        let Some(headers) = headers else {
            return Ok(plan);
        };

        for (name, value) in headers {
            let name_text = name.as_str();
            if name_text == "content-md5"
                || name_text == "content-length"
                || name_text == "transfer-encoding"
                || name_text == "x-amz-content-sha256"
                || name_text.starts_with("x-amz-checksum-")
                || name_text.starts_with("x-amz-sdk-checksum-")
            {
                return Err(S3Error::UnsupportedMultipartHeader(name.clone()));
            }

            // The request's explicit content_type argument generates this
            // header after custom headers are merged, so it remains authoritative.
            if name_text == "content-type" {
                continue;
            }

            if matches!(
                name_text,
                "x-amz-expected-bucket-owner" | "x-amz-request-payer"
            ) {
                plan.initiate.insert(name.clone(), value.clone());
                plan.part.insert(name.clone(), value.clone());
                plan.complete.insert(name.clone(), value.clone());
                plan.abort.insert(name.clone(), value.clone());
            } else if matches!(
                name_text,
                "x-amz-server-side-encryption-customer-algorithm"
                    | "x-amz-server-side-encryption-customer-key"
                    | "x-amz-server-side-encryption-customer-key-md5"
            ) {
                plan.initiate.insert(name.clone(), value.clone());
                plan.part.insert(name.clone(), value.clone());
                plan.complete.insert(name.clone(), value.clone());
            } else if matches!(name_text, "if-match" | "if-none-match") {
                plan.complete.insert(name.clone(), value.clone());
            } else {
                // PutObject properties and provider-specific custom headers are
                // applied at multipart initiation, where S3 stores object metadata.
                plan.initiate.insert(name.clone(), value.clone());
            }
        }

        Ok(plan)
    }
}

#[cfg(feature = "sync")]
fn part_from_response(response_data: ResponseData, part_number: u32) -> Result<Part, S3Error> {
    if !(200..300).contains(&response_data.status_code()) {
        return Err(response_error_from_data(response_data));
    }
    let etag = response_data.as_str()?.to_owned();
    Ok(Part { etag, part_number })
}

pub const CHUNK_SIZE: usize = 8_388_608; // 8 Mebibytes, min is 5 (5_242_880);

const DEFAULT_REQUEST_TIMEOUT: Option<Duration> = Some(Duration::from_secs(60));

#[derive(Debug, PartialEq, Eq)]
pub struct Tag {
    key: String,
    value: String,
}

impl Tag {
    pub fn key(&self) -> String {
        self.key.to_owned()
    }

    pub fn value(&self) -> String {
        self.value.to_owned()
    }
}

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
#[derive(Clone, Debug)]
pub struct Bucket {
    pub name: String,
    pub region: Region,
    credentials: Arc<RwLock<Credentials>>,
    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    credentials_refresh_gate: Arc<AsyncMutex<()>>,
    pub extra_headers: HeaderMap,
    pub extra_query: Query,
    pub request_timeout: Option<Duration>,
    path_style: bool,
    listobjects_v2: bool,
    #[cfg(feature = "with-tokio")]
    http_client: reqwest::Client,
    #[cfg(feature = "with-tokio")]
    client_options: crate::request::tokio_backend::ClientOptions,
}

impl Bucket {
    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    async fn credentials_refresh_with<F>(&self, refresh: F) -> Result<(), S3Error>
    where
        F: FnOnce(Credentials) -> Result<Credentials, crate::creds::error::CredentialsError>
            + Send
            + 'static,
    {
        let expired = {
            let credentials = self.credentials.read().await;
            credentials
                .expiration
                .is_some_and(|expiration| *expiration <= time::OffsetDateTime::now_utc())
        };
        if !expired {
            return Ok(());
        }

        let _refresh_guard = self.credentials_refresh_gate.lock().await;
        let credentials = { self.credentials.read().await.clone() };
        if !credentials
            .expiration
            .is_some_and(|expiration| *expiration <= time::OffsetDateTime::now_utc())
        {
            return Ok(());
        }

        #[cfg(feature = "with-tokio")]
        let refreshed = tokio::task::spawn_blocking(move || refresh(credentials))
            .await
            .map_err(|_| S3Error::Io(std::io::Error::other("credential refresh task failed")))??;

        #[cfg(all(not(feature = "with-tokio"), feature = "with-async-std"))]
        let refreshed = async_std::task::spawn_blocking(move || refresh(credentials)).await?;

        *self.credentials.write().await = refreshed;
        Ok(())
    }

    #[maybe_async::async_impl]
    /// Credential refreshing is done automatically, but can be manually triggered.
    pub async fn credentials_refresh(&self) -> Result<(), S3Error> {
        self.credentials_refresh_with(|mut credentials| {
            credentials.refresh()?;
            Ok(credentials)
        })
        .await
    }

    #[maybe_async::sync_impl]
    /// Credential refreshing is done automatically, but can be manually triggered.
    pub fn credentials_refresh(&self) -> Result<(), S3Error> {
        match self.credentials.write() {
            Ok(mut credentials) => Ok(credentials.refresh()?),
            Err(_) => Err(S3Error::CredentialsWriteLock),
        }
    }

    #[cfg(feature = "with-tokio")]
    pub fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }
}

fn validate_expiry(expiry_secs: u32) -> Result<(), S3Error> {
    if 604800 < expiry_secs {
        return Err(S3Error::MaxExpiry(expiry_secs));
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
    /// use std::collections::HashMap;
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// // Add optional custom queries
    /// let mut custom_queries = HashMap::new();
    /// custom_queries.insert(
    ///    "response-content-disposition".into(),
    ///    "attachment; filename=\"test.png\"".into(),
    /// );
    ///
    /// #[cfg(not(feature = "sync"))]
    /// let url = bucket.presign_get("/test.file", 86400, Some(custom_queries)).await.unwrap();
    /// #[cfg(feature = "sync")]
    /// let url = bucket.presign_get("/test.file", 86400, Some(custom_queries)).unwrap();
    /// println!("Presigned url: {}", url);
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn presign_get<S: AsRef<str>>(
        &self,
        path: S,
        expiry_secs: u32,
        custom_queries: Option<HashMap<String, String>>,
    ) -> Result<String, S3Error> {
        validate_expiry(expiry_secs)?;
        let request = RequestImpl::new(
            self,
            path.as_ref(),
            Command::PresignGet {
                expiry_secs,
                custom_queries,
            },
        )
        .await?;
        request.presigned().await
    }

    /// Get a presigned url for posting an object to a given path
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::post_policy::*;
    /// use std::borrow::Cow;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let post_policy = PostPolicy::new(86400).condition(
    ///     PostPolicyField::Key,
    ///     PostPolicyValue::StartsWith(Cow::from("user/user1/"))
    /// ).unwrap();
    ///
    /// #[cfg(not(feature = "sync"))]
    /// let presigned_post = bucket.presign_post(post_policy).await.unwrap();
    /// #[cfg(feature = "sync")]
    /// let presigned_post = bucket.presign_post(post_policy).unwrap();
    /// println!("Presigned url: {}, fields: {:?}", presigned_post.url, presigned_post.fields);
    /// }
    /// ```
    #[maybe_async::maybe_async]
    #[allow(clippy::needless_lifetimes)]
    pub async fn presign_post<'a>(
        &self,
        post_policy: PostPolicy<'a>,
    ) -> Result<PresignedPost, S3Error> {
        post_policy.sign(Box::new(self.clone())).await
    }

    /// Get a presigned url for putting object to a given path
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use http::HeaderMap;
    /// use http::header::HeaderName;
    /// #[tokio::main]
    /// async fn main() {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// // Add optional custom headers
    /// let mut custom_headers = HeaderMap::new();
    /// custom_headers.insert(
    ///    HeaderName::from_static("custom_header"),
    ///    "custom_value".parse().unwrap(),
    /// );
    ///
    /// #[cfg(not(feature = "sync"))]
    /// let url = bucket.presign_put("/test.file", 86400, Some(custom_headers), None).await.unwrap();
    /// #[cfg(feature = "sync")]
    /// let url = bucket.presign_put("/test.file", 86400, Some(custom_headers), None).unwrap();
    /// println!("Presigned url: {}", url);
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn presign_put<S: AsRef<str>>(
        &self,
        path: S,
        expiry_secs: u32,
        custom_headers: Option<HeaderMap>,
        custom_queries: Option<HashMap<String, String>>,
    ) -> Result<String, S3Error> {
        validate_expiry(expiry_secs)?;
        let request = RequestImpl::new(
            self,
            path.as_ref(),
            Command::PresignPut {
                expiry_secs,
                custom_headers,
                custom_queries,
            },
        )
        .await?;
        request.presigned().await
    }

    /// Get a presigned url for deleting object on a given path
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default().unwrap();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// #[cfg(not(feature = "sync"))]
    /// let url = bucket.presign_delete("/test.file", 86400).await.unwrap();
    /// #[cfg(feature = "sync")]
    /// let url = bucket.presign_delete("/test.file", 86400).unwrap();
    /// println!("Presigned url: {}", url);
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn presign_delete<S: AsRef<str>>(
        &self,
        path: S,
        expiry_secs: u32,
    ) -> Result<String, S3Error> {
        validate_expiry(expiry_secs)?;
        let request =
            RequestImpl::new(self, path.as_ref(), Command::PresignDelete { expiry_secs }).await?;
        request.presigned().await
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
    /// #[cfg(not(feature = "sync"))]
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
    ) -> Result<CreateBucketResponse, S3Error> {
        let mut config = config;

        // Check if we should skip location constraint for LocalStack/Minio compatibility
        // This env var allows users to create buckets on S3-compatible services that
        // don't support or require location constraints in the request body
        let skip_constraint = std::env::var("RUST_S3_SKIP_LOCATION_CONSTRAINT")
            .unwrap_or_default()
            .to_lowercase();

        if skip_constraint != "true" && skip_constraint != "1" {
            config.set_region(region.clone());
        }

        let command = Command::CreateBucket { config };
        let bucket = Bucket::new(name, region, credentials)?;
        let request = RequestImpl::new(&bucket, "", command).await?;
        let response_data = request.response_data(false).await?;
        let response_text = response_data.as_str()?;
        Ok(CreateBucketResponse {
            bucket,
            response_text: response_text.to_string(),
            response_code: response_data.status_code(),
        })
    }

    /// Get a list of all existing buckets in the region
    /// that are accessible by the given credentials.
    /// ```no_run
    /// use s3::{Bucket, BucketConfiguration};
    /// use s3::creds::Credentials;
    /// use s3::region::Region;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let region = Region::Custom {
    ///   region: "eu-central-1".to_owned(),
    ///   endpoint: "http://localhost:9000".to_owned()
    /// };
    /// let credentials = Credentials::default()?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// #[cfg(not(feature = "sync"))]
    /// let response = Bucket::list_buckets(region.clone(), credentials.clone()).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response = Bucket::list_buckets(region, credentials)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response = Bucket::list_buckets_blocking(region, credentials)?;
    ///
    /// let found_buckets = response.bucket_names().collect::<Vec<String>>();
    /// println!("found buckets: {:#?}", found_buckets);
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn list_buckets(
        region: Region,
        credentials: Credentials,
    ) -> Result<crate::bucket_ops::ListBucketsResponse, S3Error> {
        let dummy_bucket = Bucket::new("", region, credentials)?.with_path_style();
        dummy_bucket._list_buckets().await
    }

    /// Internal helper method that performs the actual bucket listing operation.
    /// Used by the public `list_buckets` method to retrieve the list of buckets for the configured client.
    #[maybe_async::maybe_async]
    async fn _list_buckets(&self) -> Result<crate::bucket_ops::ListBucketsResponse, S3Error> {
        let request = RequestImpl::new(self, "", Command::ListBuckets).await?;
        let response = request.response_data(false).await?;

        Ok(quick_xml::de::from_str::<
            crate::bucket_ops::ListBucketsResponse,
        >(response.as_str()?)?)
    }

    /// Determine whether the instantiated bucket exists.
    /// ```no_run
    /// use s3::{Bucket, BucketConfiguration};
    /// use s3::creds::Credentials;
    /// use s3::region::Region;
    /// use anyhow::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let bucket_name = "some-bucket-that-is-known-to-exist";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    ///
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// #[cfg(not(feature = "sync"))]
    /// let exists = bucket.exists().await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let exists = bucket.exists()?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let exists = bucket.exists_blocking()?;
    ///
    /// assert_eq!(exists, true);
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn exists(&self) -> Result<bool, S3Error> {
        let mut dummy_bucket = self.clone();
        dummy_bucket.name = "".into();

        let response = dummy_bucket._list_buckets().await?;

        Ok(response
            .bucket_names()
            .collect::<std::collections::HashSet<String>>()
            .contains(&self.name))
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
    /// #[cfg(not(feature = "sync"))]
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
        config: BucketConfiguration,
    ) -> Result<CreateBucketResponse, S3Error> {
        let mut config = config;

        // Check if we should skip location constraint for LocalStack/Minio compatibility
        // This env var allows users to create buckets on S3-compatible services that
        // don't support or require location constraints in the request body
        let skip_constraint = std::env::var("RUST_S3_SKIP_LOCATION_CONSTRAINT")
            .unwrap_or_default()
            .to_lowercase();

        if skip_constraint != "true" && skip_constraint != "1" {
            config.set_region(region.clone());
        }

        let command = Command::CreateBucket { config };
        let bucket = Bucket::new(name, region, credentials)?.with_path_style();
        let request = RequestImpl::new(&bucket, "", command).await?;
        let response_data = request.response_data(false).await?;
        let response_text = response_data.to_string()?;

        Ok(CreateBucketResponse {
            bucket,
            response_text,
            response_code: response_data.status_code(),
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
    /// #[cfg(not(feature = "sync"))]
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
    pub async fn delete(&self) -> Result<u16, S3Error> {
        let command = Command::DeleteBucket;
        let request = RequestImpl::new(self, "", command).await?;
        let response_data = request.response_data(false).await?;
        Ok(response_data.status_code())
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
    pub fn new(
        name: &str,
        region: Region,
        credentials: Credentials,
    ) -> Result<Box<Bucket>, S3Error> {
        #[cfg(feature = "with-tokio")]
        let options = ClientOptions::default();

        Ok(Box::new(Bucket {
            name: name.into(),
            region,
            credentials: Arc::new(RwLock::new(credentials)),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: Arc::new(AsyncMutex::new(())),
            extra_headers: HeaderMap::new(),
            extra_query: HashMap::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            path_style: false,
            listobjects_v2: true,
            #[cfg(feature = "with-tokio")]
            http_client: client(&options)?,
            #[cfg(feature = "with-tokio")]
            client_options: options,
        }))
    }

    /// Instantiate a public existing `Bucket`.
    ///
    /// # Example
    /// ```no_run
    /// use s3::bucket::Bucket;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    ///
    /// let bucket = Bucket::new_public(bucket_name, region).unwrap();
    /// ```
    pub fn new_public(name: &str, region: Region) -> Result<Bucket, S3Error> {
        #[cfg(feature = "with-tokio")]
        let options = ClientOptions::default();

        Ok(Bucket {
            name: name.into(),
            region,
            credentials: Arc::new(RwLock::new(Credentials::anonymous()?)),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: Arc::new(AsyncMutex::new(())),
            extra_headers: HeaderMap::new(),
            extra_query: HashMap::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            path_style: false,
            listobjects_v2: true,
            #[cfg(feature = "with-tokio")]
            http_client: client(&options)?,
            #[cfg(feature = "with-tokio")]
            client_options: options,
        })
    }

    pub fn with_path_style(&self) -> Box<Bucket> {
        Box::new(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: true,
            listobjects_v2: self.listobjects_v2,
            #[cfg(feature = "with-tokio")]
            http_client: self.http_client(),
            #[cfg(feature = "with-tokio")]
            client_options: self.client_options.clone(),
        })
    }

    pub fn with_extra_headers(&self, extra_headers: HeaderMap) -> Result<Bucket, S3Error> {
        Ok(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers,
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            #[cfg(feature = "with-tokio")]
            http_client: self.http_client(),
            #[cfg(feature = "with-tokio")]
            client_options: self.client_options.clone(),
        })
    }

    pub fn with_extra_query(
        &self,
        extra_query: HashMap<String, String>,
    ) -> Result<Bucket, S3Error> {
        Ok(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query,
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            #[cfg(feature = "with-tokio")]
            http_client: self.http_client(),
            #[cfg(feature = "with-tokio")]
            client_options: self.client_options.clone(),
        })
    }

    #[cfg(not(feature = "with-tokio"))]
    pub fn with_request_timeout(&self, request_timeout: Duration) -> Result<Box<Bucket>, S3Error> {
        Ok(Box::new(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: Some(request_timeout),
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
        }))
    }

    #[cfg(feature = "with-tokio")]
    pub fn with_request_timeout(&self, request_timeout: Duration) -> Result<Box<Bucket>, S3Error> {
        Ok(Box::new(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: Some(request_timeout),
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            #[cfg(feature = "with-tokio")]
            http_client: self.http_client(),
            #[cfg(feature = "with-tokio")]
            client_options: self.client_options.clone(),
        }))
    }

    pub fn with_listobjects_v1(&self) -> Bucket {
        Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: false,
            #[cfg(feature = "with-tokio")]
            http_client: self.http_client(),
            #[cfg(feature = "with-tokio")]
            client_options: self.client_options.clone(),
        }
    }

    /// Configures a bucket to accept invalid SSL certificates and hostnames.
    ///
    /// This method is available only when either the `tokio-native-tls` or `tokio-rustls-tls` feature is enabled.
    ///
    /// # Parameters
    ///
    /// - `accept_invalid_certs`: A boolean flag that determines whether the client should accept invalid SSL certificates.
    /// - `accept_invalid_hostnames`: A boolean flag that determines whether the client should accept invalid hostnames.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the newly configured `Bucket` instance if successful, or an `S3Error` if an error occurs during client configuration.
    ///
    /// # Errors
    ///
    /// This function returns an `S3Error` if the HTTP client configuration fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use s3::bucket::Bucket;
    /// # use s3::error::S3Error;
    /// # use s3::creds::Credentials;
    /// # use s3::Region;
    /// # use std::str::FromStr;
    ///
    /// # fn example() -> Result<(), S3Error> {
    /// let bucket = Bucket::new("my-bucket", Region::from_str("us-east-1")?, Credentials::default()?)?
    ///     .set_dangerous_config(true, true)?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub fn set_dangerous_config(
        &self,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Result<Bucket, S3Error> {
        let mut options = self.client_options.clone();
        options.accept_invalid_certs = accept_invalid_certs;
        options.accept_invalid_hostnames = accept_invalid_hostnames;

        Ok(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client: client(&options)?,
            client_options: options,
        })
    }

    /// Deprecated alias for [`Bucket::set_dangerous_config`].
    #[deprecated(
        since = "0.37.3",
        note = "use `set_dangerous_config`; this misspelled method remains for compatibility"
    )]
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub fn set_dangereous_config(
        &self,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Result<Bucket, S3Error> {
        self.set_dangerous_config(accept_invalid_certs, accept_invalid_hostnames)
    }

    #[cfg(feature = "with-tokio")]
    pub fn set_proxy(&self, proxy: reqwest::Proxy) -> Result<Bucket, S3Error> {
        let mut options = self.client_options.clone();
        options.proxy = Some(proxy);

        Ok(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
            credentials_refresh_gate: self.credentials_refresh_gate.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client: client(&options)?,
            client_options: options,
        })
    }

    /// Copy file from an S3 path, internally within the same bucket.
    ///
    /// Returns an error if S3 returns an XML error document with HTTP status 200.
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
    /// #[cfg(not(feature = "sync"))]
    /// let code = bucket.copy_object_internal("/from.file", "/to.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let code = bucket.copy_object_internal("/from.file", "/to.file")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn copy_object_internal<F: AsRef<str>, T: AsRef<str>>(
        &self,
        from: F,
        to: T,
    ) -> Result<u16, S3Error> {
        let fq_from = {
            let from = from.as_ref();
            let from = from.strip_prefix('/').unwrap_or(from);
            format!("{bucket}/{path}", bucket = self.name(), path = from)
        };
        self.copy_object(fq_from, to).await
    }

    #[maybe_async::maybe_async]
    async fn copy_object<F: AsRef<str>, T: AsRef<str>>(
        &self,
        from: F,
        to: T,
    ) -> Result<u16, S3Error> {
        let command = Command::CopyObject {
            from: from.as_ref(),
        };
        let request = RequestImpl::new(self, to.as_ref(), command).await?;
        let response_data = request.response_data(false).await?;
        let response_data = validate_success_xml_response(response_data, "CopyObjectResult")?;
        Ok(response_data.status_code())
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.get_object("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.get_object("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.get_object_blocking("/test.file")?;
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object<S: AsRef<str>>(&self, path: S) -> Result<ResponseData, S3Error> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(false).await
    }

    #[maybe_async::maybe_async]
    pub async fn get_object_attributes<S: AsRef<str>>(
        &self,
        path: S,
        expected_bucket_owner: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectAttributesOutput, S3Error> {
        let command = Command::GetObjectAttributes {
            expected_bucket_owner: expected_bucket_owner.to_string(),
            version_id,
        };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;

        let response = request.response_data(false).await?;

        Ok(quick_xml::de::from_str::<GetObjectAttributesOutput>(
            response.as_str()?,
        )?)
    }

    /// Checks if an object exists at the specified S3 path.
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
    /// #[cfg(not(feature = "sync"))]
    /// let exists = bucket.object_exists("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let exists = bucket.object_exists("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let exists = bucket.object_exists_blocking("/test.file")?;
    ///
    /// if exists {
    ///     println!("Object exists.");
    /// } else {
    ///     println!("Object does not exist.");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// This function will return an `Err` if the request to the S3 service fails or if there is an unexpected error.
    /// It will return `Ok(false)` if the object does not exist (i.e., the server returns a 404 status code).
    #[maybe_async::maybe_async]
    pub async fn object_exists<S: AsRef<str>>(&self, path: S) -> Result<bool, S3Error> {
        let command = Command::HeadObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        let status_code = request.response_status().await?;
        Ok(status_code != 404)
    }

    #[maybe_async::maybe_async]
    pub async fn put_bucket_cors(
        &self,
        expected_bucket_owner: &str,
        cors_config: &CorsConfiguration,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutBucketCors {
            expected_bucket_owner: expected_bucket_owner.to_string(),
            configuration: cors_config.clone(),
        };
        let request = RequestImpl::new(self, "", command).await?;
        request.response_data(false).await
    }

    #[maybe_async::maybe_async]
    pub async fn get_bucket_cors(
        &self,
        expected_bucket_owner: &str,
    ) -> Result<CorsConfiguration, S3Error> {
        let command = Command::GetBucketCors {
            expected_bucket_owner: expected_bucket_owner.to_string(),
        };
        let request = RequestImpl::new(self, "", command).await?;
        let response = request.response_data(false).await?;
        Ok(quick_xml::de::from_str::<CorsConfiguration>(
            response.as_str()?,
        )?)
    }

    #[maybe_async::maybe_async]
    pub async fn delete_bucket_cors(
        &self,
        expected_bucket_owner: &str,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::DeleteBucketCors {
            expected_bucket_owner: expected_bucket_owner.to_string(),
        };
        let request = RequestImpl::new(self, "", command).await?;
        request.response_data(false).await
    }

    #[maybe_async::maybe_async]
    pub async fn get_bucket_lifecycle(&self) -> Result<BucketLifecycleConfiguration, S3Error> {
        let request = RequestImpl::new(self, "", Command::GetBucketLifecycle).await?;
        let response = request.response_data(false).await?;
        Ok(quick_xml::de::from_str::<BucketLifecycleConfiguration>(
            response.as_str()?,
        )?)
    }

    #[maybe_async::maybe_async]
    pub async fn put_bucket_lifecycle(
        &self,
        lifecycle_config: BucketLifecycleConfiguration,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutBucketLifecycle {
            configuration: lifecycle_config,
        };
        let request = RequestImpl::new(self, "", command).await?;
        request.response_data(false).await
    }

    #[maybe_async::maybe_async]
    pub async fn delete_bucket_lifecycle(&self) -> Result<ResponseData, S3Error> {
        let request = RequestImpl::new(self, "", Command::DeleteBucketLifecycle).await?;
        request.response_data(false).await
    }

    /// Gets torrent from an S3 path.
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.get_object_torrent("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.get_object_torrent("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.get_object_torrent_blocking("/test.file")?;
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn get_object_torrent<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::GetObjectTorrent;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(false).await
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.get_object_range("/test.file", 0, Some(31)).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.get_object_range("/test.file", 0, Some(31))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.get_object_range_blocking("/test.file", 0, Some(31))?;
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
    ) -> Result<ResponseData, S3Error> {
        if let Some(end) = end {
            assert!(start <= end);
        }

        let command = Command::GetObjectRange { start, end };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(false).await
    }

    /// Stream range of bytes from S3 path to a local file, generic over T: Write.
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
    /// let mut async_output_file = tokio::fs::File::create("async_output_file").await.expect("Unable to create file");
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_output_file = async_std::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// let start = 0;
    /// let end = Some(1024);
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let status_code = bucket.get_object_range_to_writer("/test.file", start, end, &mut async_output_file).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket.get_object_range_to_writer("/test.file", start, end, &mut output_file)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features. Based of the async branch
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.get_object_range_to_writer_blocking("/test.file", start, end, &mut async_output_file)?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::async_impl]
    pub async fn get_object_range_to_writer<T, S>(
        &self,
        path: S,
        start: u64,
        end: Option<u64>,
        writer: &mut T,
    ) -> Result<u16, S3Error>
    where
        T: AsyncWrite + Send + Unpin + ?Sized,
        S: AsRef<str>,
    {
        if let Some(end) = end {
            assert!(start <= end);
        }

        let command = Command::GetObjectRange { start, end };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data_to_writer(writer).await
    }

    #[maybe_async::sync_impl]
    pub fn get_object_range_to_writer<T: std::io::Write + Send + ?Sized, S: AsRef<str>>(
        &self,
        path: S,
        start: u64,
        end: Option<u64>,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        if let Some(end) = end {
            assert!(start <= end);
        }

        let command = Command::GetObjectRange { start, end };
        let request = RequestImpl::new(self, path.as_ref(), command)?;
        request.response_data_to_writer(writer)
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
    /// let mut async_output_file = tokio::fs::File::create("async_output_file").await.expect("Unable to create file");
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_output_file = async_std::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// let status_code = bucket.get_object_to_writer("/test.file", &mut async_output_file).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket.get_object_to_writer("/test.file", &mut output_file)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features. Based of the async branch
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.get_object_to_writer_blocking("/test.file", &mut async_output_file)?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::async_impl]
    pub async fn get_object_to_writer<T: AsyncWrite + Send + Unpin + ?Sized, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data_to_writer(writer).await
    }

    #[maybe_async::sync_impl]
    pub fn get_object_to_writer<T: std::io::Write + Send + ?Sized, S: AsRef<str>>(
        &self,
        path: S,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command)?;
        request.response_data_to_writer(writer)
    }

    /// Stream file from S3 path to a local file using an async stream.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    /// #[cfg(feature = "with-tokio")]
    /// use tokio_stream::StreamExt;
    /// #[cfg(feature = "with-tokio")]
    /// use tokio::io::AsyncWriteExt;
    /// #[cfg(feature = "with-async-std")]
    /// use async_std::stream::StreamExt;
    /// #[cfg(feature = "with-async-std")]
    /// use async_std::io::WriteExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse()?;
    /// let credentials = Credentials::default()?;
    /// let bucket = Bucket::new(bucket_name, region, credentials)?;
    /// let path = "path";
    ///
    /// let mut response_data_stream = bucket.get_object_stream(path).await?;
    ///
    /// #[cfg(feature = "with-tokio")]
    /// let mut async_output_file = tokio::fs::File::create("async_output_file").await.expect("Unable to create file");
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_output_file = async_std::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// while let Some(chunk) = response_data_stream.bytes().next().await {
    ///     async_output_file.write_all(&chunk.unwrap()).await?;
    /// }
    ///
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    pub async fn get_object_stream<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<ResponseDataStream, S3Error> {
        let command = Command::GetObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data_to_stream().await
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
    /// #[cfg(feature = "with-tokio")]
    /// let mut async_reader = tokio::fs::File::open(path).await?;
    /// #[cfg(feature = "with-tokio")]
    /// let status_code = bucket.put_object_stream(&mut async_reader, "/path").await?;
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_reader = async_std::fs::File::open(path).await?;
    /// #[cfg(feature = "with-async-std")]
    /// let status_code = bucket.put_object_stream(&mut async_reader, "/path").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let mut sync_reader = File::open(path)?;
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket.put_object_stream(&mut sync_reader, "/path")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.put_object_stream_blocking(&mut async_reader, "/path")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::async_impl]
    pub async fn put_object_stream<R: AsyncRead + Unpin + ?Sized>(
        &self,
        reader: &mut R,
        s3_path: impl AsRef<str>,
    ) -> Result<PutStreamResponse, S3Error> {
        self._put_object_stream_with_content_type(
            reader,
            s3_path.as_ref(),
            "application/octet-stream",
        )
        .await
    }

    /// Create a builder for streaming PUT operations with custom options
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use anyhow::Result;
    ///
    /// # #[cfg(feature = "with-tokio")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # use tokio::fs::File;
    ///
    /// let bucket = Bucket::new("my-bucket", "us-east-1".parse()?, Credentials::default()?)?;
    ///
    /// # #[cfg(feature = "with-tokio")]
    /// let mut file = File::open("large-file.zip").await?;
    ///
    /// // Stream upload with custom headers using builder pattern
    /// let response = bucket.put_object_stream_builder("/large-file.zip")
    ///     .with_content_type("application/zip")
    ///     .with_cache_control("public, max-age=3600")?
    ///     .with_metadata("uploaded-by", "stream-builder")?
    ///     .execute_stream(&mut file)
    ///     .await?;
    /// #
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "with-tokio"))]
    /// # fn main() {}
    /// ```
    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    pub fn put_object_stream_builder<S: AsRef<str>>(
        &self,
        path: S,
    ) -> crate::put_object_request::PutObjectStreamRequest<'_> {
        crate::put_object_request::PutObjectStreamRequest::new(self, path)
    }

    #[maybe_async::sync_impl]
    pub fn put_object_stream<R: Read>(
        &self,
        reader: &mut R,
        s3_path: impl AsRef<str>,
    ) -> Result<u16, S3Error> {
        self._put_object_stream_with_content_type(
            reader,
            s3_path.as_ref(),
            "application/octet-stream",
        )
    }

    /// Stream file from local path to s3, generic over T: Write with explicit content type.
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
    /// #[cfg(feature = "with-tokio")]
    /// let mut async_reader = tokio::fs::File::open(path).await?;
    ///
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_reader = async_std::fs::File::open(path).await?;
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// // Generic over std::io::Read
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type(&mut async_reader, "/path", "application/octet-stream")
    ///     .await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// // Generic over std::io::Read
    /// let mut sync_reader = File::open(path)?;
    /// #[cfg(feature = "sync")]
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type(&mut sync_reader, "/path", "application/octet-stream")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type_blocking(&mut async_reader, "/path", "application/octet-stream")?;
    ///
    /// #
    /// # Ok(())
    /// # }
    /// ```
    /// After multipart initiation, returned errors trigger a best-effort abort.
    /// Cancelling the operation or losing the completion response can leave the
    /// remote upload outcome uncertain.
    #[maybe_async::async_impl]
    pub async fn put_object_stream_with_content_type<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        s3_path: impl AsRef<str>,
        content_type: impl AsRef<str>,
    ) -> Result<PutStreamResponse, S3Error> {
        self._put_object_stream_with_content_type(reader, s3_path.as_ref(), content_type.as_ref())
            .await
    }

    #[maybe_async::sync_impl]
    pub fn put_object_stream_with_content_type<R: Read>(
        &self,
        reader: &mut R,
        s3_path: impl AsRef<str>,
        content_type: impl AsRef<str>,
    ) -> Result<u16, S3Error> {
        self._put_object_stream_with_content_type(reader, s3_path.as_ref(), content_type.as_ref())
    }

    #[maybe_async::maybe_async]
    async fn make_multipart_request(
        &self,
        path: &str,
        chunk: Vec<u8>,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
        custom_headers: &HeaderMap,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutObject {
            content: &chunk,
            multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
            custom_headers: Some(custom_headers.clone()),
            content_type,
        };
        let request = RequestImpl::new(self, path, command).await?;
        request.response_data(true).await
    }

    #[maybe_async::maybe_async]
    async fn abort_upload_after_failure<T>(
        &self,
        path: &str,
        upload_id: &str,
        abort_headers: &HeaderMap,
        error: S3Error,
    ) -> Result<T, S3Error> {
        // Cleanup is best effort. Keep the operation's original error even if
        // the abort fails (for example, if completion already reached S3).
        let bucket = self.with_overlaid_extra_headers(abort_headers);
        let _ = bucket.abort_upload(path, upload_id).await;
        Err(error)
    }

    fn with_overlaid_extra_headers(&self, overlay: &HeaderMap) -> Bucket {
        let mut bucket = self.clone();
        bucket.extra_headers.extend(overlay.clone());
        bucket
    }

    #[maybe_async::async_impl]
    async fn _put_object_stream_with_content_type<R: AsyncRead + Unpin + ?Sized>(
        &self,
        reader: &mut R,
        s3_path: &str,
        content_type: &str,
    ) -> Result<PutStreamResponse, S3Error> {
        self._put_object_stream_with_content_type_and_headers(reader, s3_path, content_type, None)
            .await
    }

    /// Calculate the maximum number of concurrent chunks based on available memory.
    /// Returns a per-upload count between 2 and 10, defaulting to 3 if detection fails.
    /// This sizing input is not a process-wide memory or RSS limit.
    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    fn max_concurrent_chunks_for_memory(available_memory: u64) -> usize {
        const DEFAULT_CONCURRENT_CHUNKS: usize = 3;
        const MAX_CONCURRENT_CHUNKS: usize = 10;

        if available_memory == 0 {
            return DEFAULT_CONCURRENT_CHUNKS;
        }

        let memory_per_chunk = CHUNK_SIZE as u64 * 3;
        (available_memory / memory_per_chunk).clamp(2, MAX_CONCURRENT_CHUNKS as u64) as usize
    }

    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    fn calculate_max_concurrent_chunks() -> usize {
        // Create a new System instance and refresh memory info
        let mut system = System::new();
        system.refresh_memory_specifics(MemoryRefreshKind::everything());

        // Get available memory in bytes
        let available_memory = system.available_memory();

        Self::max_concurrent_chunks_for_memory(available_memory)
    }

    #[maybe_async::async_impl]
    pub(crate) async fn _put_object_stream_with_content_type_and_headers<
        R: AsyncRead + Unpin + ?Sized,
    >(
        &self,
        reader: &mut R,
        s3_path: &str,
        content_type: &str,
        custom_headers: Option<http::HeaderMap>,
    ) -> Result<PutStreamResponse, S3Error> {
        // If the file is smaller CHUNK_SIZE, just do a regular upload.
        // Otherwise perform a multi-part upload.
        let first_chunk = crate::utils::read_chunk_async(reader).await?;
        // println!("First chunk size: {}", first_chunk.len());
        if first_chunk.len() < CHUNK_SIZE {
            let total_size = first_chunk.len();
            // Use the builder pattern for small files
            let mut builder = self
                .put_object_builder(s3_path, first_chunk.as_slice())
                .with_content_type(content_type);

            // Add custom headers if provided
            if let Some(headers) = custom_headers {
                builder = builder.with_headers(headers);
            }

            let response_data = builder.execute().await?;
            if response_data.status_code() >= 300 {
                return Err(error_from_response_data(response_data)?);
            }
            return Ok(PutStreamResponse::new(
                response_data.status_code(),
                total_size,
            ));
        }

        let header_plan = MultipartHeaderPlan::from_headers(custom_headers.as_ref())?;
        let initiate_bucket = self.with_overlaid_extra_headers(&header_plan.initiate);
        let msg = initiate_bucket
            .initiate_multipart_upload(s3_path, content_type)
            .await?;
        let path = msg.key;
        let upload_id = &msg.upload_id;

        // Determine max concurrent chunks based on available memory
        let max_concurrent_chunks = Self::calculate_max_concurrent_chunks();

        // Use FuturesUnordered for bounded parallelism
        use futures_util::FutureExt;
        use futures_util::stream::{FuturesUnordered, StreamExt};

        // Keep all part futures inside this scope. If reading or a part fails,
        // leaving the scope drops the outstanding futures before abort starts.
        let upload_result: Result<(Vec<(u32, String)>, usize), S3Error> = async {
            let mut part_number: u32 = 0;
            let mut total_size = 0;
            let mut etags = Vec::new();
            let mut active_uploads: FuturesUnordered<
                futures_util::future::BoxFuture<'_, (u32, Result<ResponseData, S3Error>)>,
            > = FuturesUnordered::new();
            let mut reading_done = false;

            part_number += 1;
            total_size += first_chunk.len();
            if first_chunk.len() < CHUNK_SIZE {
                reading_done = true;
            }

            let path_clone = path.clone();
            let upload_id_clone = upload_id.clone();
            let content_type_clone = content_type.to_string();
            let part_headers = header_plan.part.clone();
            let bucket_clone = self.clone();

            active_uploads.push(
                async move {
                    let result = bucket_clone
                        .make_multipart_request(
                            &path_clone,
                            first_chunk,
                            1,
                            &upload_id_clone,
                            &content_type_clone,
                            &part_headers,
                        )
                        .await;
                    (1, result)
                }
                .boxed(),
            );

            while !active_uploads.is_empty() || !reading_done {
                while active_uploads.len() < max_concurrent_chunks && !reading_done {
                    let chunk = crate::utils::read_chunk_async(reader).await?;
                    let chunk_len = chunk.len();

                    if chunk_len == 0 {
                        reading_done = true;
                        break;
                    }

                    total_size += chunk_len;
                    part_number += 1;

                    if chunk_len < CHUNK_SIZE {
                        reading_done = true;
                    }

                    let current_part = part_number;
                    let path_clone = path.clone();
                    let upload_id_clone = upload_id.clone();
                    let content_type_clone = content_type.to_string();
                    let part_headers = header_plan.part.clone();
                    let bucket_clone = self.clone();

                    active_uploads.push(
                        async move {
                            let result = bucket_clone
                                .make_multipart_request(
                                    &path_clone,
                                    chunk,
                                    current_part,
                                    &upload_id_clone,
                                    &content_type_clone,
                                    &part_headers,
                                )
                                .await;
                            (current_part, result)
                        }
                        .boxed(),
                    );
                }

                if let Some((part_num, result)) = active_uploads.next().await {
                    let response_data = result?;
                    if !(200..300).contains(&response_data.status_code()) {
                        return Err(response_error_from_data(response_data));
                    }

                    let etag = response_data.as_str()?;
                    etags.push((part_num, etag.to_string()));
                }
            }

            Ok((etags, total_size))
        }
        .await;

        let (mut etags, total_size) = match upload_result {
            Ok(result) => result,
            Err(error) => {
                return self
                    .abort_upload_after_failure(&path, upload_id, &header_plan.abort, error)
                    .await;
            }
        };

        // Sort etags by part number to ensure correct order
        etags.sort_by_key(|k| k.0);
        let etags: Vec<String> = etags.into_iter().map(|(_, etag)| etag).collect();

        // Finish the upload
        let inner_data = etags
            .into_iter()
            .enumerate()
            .map(|(i, x)| Part {
                etag: x,
                part_number: i as u32 + 1,
            })
            .collect::<Vec<Part>>();
        let complete_bucket = self.with_overlaid_extra_headers(&header_plan.complete);
        let response_data = match complete_bucket
            .complete_multipart_upload(&path, &msg.upload_id, inner_data)
            .await
        {
            Ok(response_data) if (200..300).contains(&response_data.status_code()) => response_data,
            Ok(response_data) => {
                let error = response_error_from_data(response_data);
                return self
                    .abort_upload_after_failure(&path, upload_id, &header_plan.abort, error)
                    .await;
            }
            Err(error) => {
                return self
                    .abort_upload_after_failure(&path, upload_id, &header_plan.abort, error)
                    .await;
            }
        };

        Ok(PutStreamResponse::new(
            response_data.status_code(),
            total_size,
        ))
    }

    #[maybe_async::sync_impl]
    fn _put_object_stream_with_content_type<R: Read + ?Sized>(
        &self,
        reader: &mut R,
        s3_path: &str,
        content_type: &str,
    ) -> Result<u16, S3Error> {
        let msg = self.initiate_multipart_upload(s3_path, content_type)?;
        let path = msg.key;
        let upload_id = &msg.upload_id;

        let mut needs_abort = true;
        let upload_result = (|| {
            let mut part_number: u32 = 0;
            let mut etags = Vec::new();
            loop {
                let chunk = crate::utils::read_chunk(reader)?;

                if chunk.len() < CHUNK_SIZE {
                    if part_number == 0 {
                        // Files is not big enough for multipart upload, going with regular put_object.
                        // The upload is already aborted, so a fallback PUT error must not abort again.
                        needs_abort = false;
                        self.abort_upload(&path, upload_id)?;
                        return Ok(self.put_object(s3_path, chunk.as_slice())?.status_code());
                    }

                    part_number += 1;
                    let response_data = self.make_multipart_request(
                        &path,
                        chunk,
                        part_number,
                        upload_id,
                        content_type,
                        &HeaderMap::new(),
                    )?;
                    let part = part_from_response(response_data, part_number)?;
                    etags.push(part.etag);
                    let inner_data = etags
                        .into_iter()
                        .enumerate()
                        .map(|(i, x)| Part {
                            etag: x,
                            part_number: i as u32 + 1,
                        })
                        .collect::<Vec<Part>>();
                    let response_data =
                        self.complete_multipart_upload(&path, upload_id, inner_data)?;
                    if !(200..300).contains(&response_data.status_code()) {
                        return Err(response_error_from_data(response_data));
                    }
                    return Ok(response_data.status_code());
                }

                part_number += 1;
                let response_data = self.make_multipart_request(
                    &path,
                    chunk,
                    part_number,
                    upload_id,
                    content_type,
                    &HeaderMap::new(),
                )?;
                let part = part_from_response(response_data, part_number)?;
                etags.push(part.etag);
            }
        })();

        match upload_result {
            Ok(status_code) => Ok(status_code),
            Err(error) if needs_abort => {
                self.abort_upload_after_failure(&path, upload_id, &HeaderMap::new(), error)
            }
            Err(error) => Err(error),
        }
    }

    /// Initiate multipart upload to s3.
    #[maybe_async::async_impl]
    pub async fn initiate_multipart_upload(
        &self,
        s3_path: &str,
        content_type: &str,
    ) -> Result<InitiateMultipartUploadResponse, S3Error> {
        let command = Command::InitiateMultipartUpload { content_type };
        let request = RequestImpl::new(self, s3_path, command).await?;
        let response_data = request.response_data(false).await?;
        if response_data.status_code() >= 300 {
            return Err(error_from_response_data(response_data)?);
        }

        let msg: InitiateMultipartUploadResponse =
            quick_xml::de::from_str(response_data.as_str()?)?;
        Ok(msg)
    }

    #[maybe_async::sync_impl]
    pub fn initiate_multipart_upload(
        &self,
        s3_path: &str,
        content_type: &str,
    ) -> Result<InitiateMultipartUploadResponse, S3Error> {
        let command = Command::InitiateMultipartUpload { content_type };
        let request = RequestImpl::new(self, s3_path, command)?;
        let response_data = request.response_data(false)?;
        if response_data.status_code() >= 300 {
            return Err(error_from_response_data(response_data)?);
        }

        let msg: InitiateMultipartUploadResponse =
            quick_xml::de::from_str(response_data.as_str()?)?;
        Ok(msg)
    }

    /// Upload a streamed multipart chunk to s3 using a previously initiated multipart upload
    #[maybe_async::async_impl]
    pub async fn put_multipart_stream<R: Read + Unpin>(
        &self,
        reader: &mut R,
        path: &str,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<Part, S3Error> {
        let chunk = crate::utils::read_chunk(reader)?;
        self.put_multipart_chunk(chunk, path, part_number, upload_id, content_type)
            .await
    }

    #[maybe_async::sync_impl]
    pub async fn put_multipart_stream<R: Read + Unpin>(
        &self,
        reader: &mut R,
        path: &str,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<Part, S3Error> {
        let chunk = crate::utils::read_chunk(reader)?;
        self.put_multipart_chunk(&chunk, path, part_number, upload_id, content_type)
    }

    /// Upload a buffered multipart chunk to s3 using a previously initiated multipart upload
    #[maybe_async::async_impl]
    pub async fn put_multipart_chunk(
        &self,
        chunk: Vec<u8>,
        path: &str,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<Part, S3Error> {
        let command = Command::PutObject {
            // part_number,
            content: &chunk,
            multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
            custom_headers: None,
            content_type,
        };
        let request = RequestImpl::new(self, path, command).await?;
        let response_data = request.response_data(true).await?;
        if !(200..300).contains(&response_data.status_code()) {
            // if chunk upload failed - abort the upload
            match self.abort_upload(path, upload_id).await {
                Ok(_) => {
                    return Err(error_from_response_data(response_data)?);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        let etag = response_data.as_str()?;
        Ok(Part {
            etag: etag.to_string(),
            part_number,
        })
    }

    #[maybe_async::sync_impl]
    pub fn put_multipart_chunk(
        &self,
        chunk: &[u8],
        path: &str,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<Part, S3Error> {
        let command = Command::PutObject {
            // part_number,
            content: chunk,
            multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
            custom_headers: None,
            content_type,
        };
        let request = RequestImpl::new(self, path, command)?;
        let response_data = request.response_data(true)?;
        if !(200..300).contains(&response_data.status_code()) {
            // if chunk upload failed - abort the upload
            match self.abort_upload(path, upload_id) {
                Ok(_) => {
                    return Err(error_from_response_data(response_data)?);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        let etag = response_data.as_str()?;
        Ok(Part {
            etag: etag.to_string(),
            part_number,
        })
    }

    /// Completes a previously initiated multipart upload, with optional final data chunks.
    ///
    /// Returns an error if S3 returns an XML error document with HTTP status 200.
    #[maybe_async::async_impl]
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<Part>,
    ) -> Result<ResponseData, S3Error> {
        let data = CompleteMultipartUploadData { parts };
        let complete = Command::CompleteMultipartUpload { upload_id, data };
        let complete_request = RequestImpl::new(self, path, complete).await?;
        let response_data = complete_request.response_data(false).await?;
        validate_success_xml_response(response_data, "CompleteMultipartUploadResult")
    }

    #[maybe_async::sync_impl]
    pub fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<Part>,
    ) -> Result<ResponseData, S3Error> {
        let data = CompleteMultipartUploadData { parts };
        let complete = Command::CompleteMultipartUpload { upload_id, data };
        let complete_request = RequestImpl::new(self, path, complete)?;
        let response_data = complete_request.response_data(false)?;
        validate_success_xml_response(response_data, "CompleteMultipartUploadResult")
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
    /// #[cfg(not(feature = "sync"))]
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
    pub async fn location(&self) -> Result<(Region, u16), S3Error> {
        let request = RequestImpl::new(self, "?location", Command::GetBucketLocation).await?;
        let response_data = request.response_data(false).await?;
        let region_string = String::from_utf8_lossy(response_data.as_slice());
        let region = match quick_xml::de::from_reader(region_string.as_bytes()) {
            Ok(r) => {
                let location_result: BucketLocationResult = r;
                location_result.region.parse()?
            }
            Err(e) => {
                if response_data.status_code() == 200 {
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
        Ok((region, response_data.status_code()))
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.delete_object("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.delete_object("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.delete_object_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object<S: AsRef<str>>(&self, path: S) -> Result<ResponseData, S3Error> {
        let command = Command::DeleteObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(false).await
    }

    /// Delete multiple objects from S3 using the Multi-Object Delete API.
    ///
    /// If more than 1000 objects are provided, they are automatically batched
    /// into multiple requests (S3 allows at most 1000 keys per request).
    /// Results from all batches are combined into a single response.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// use s3::bucket::Bucket;
    /// use s3::creds::Credentials;
    /// use s3::serde_types::ObjectIdentifier;
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
    /// let objects = vec![
    ///     ObjectIdentifier::new("file1.txt"),
    ///     ObjectIdentifier::new("file2.txt"),
    ///     ObjectIdentifier::new("file3.txt"),
    /// ];
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// #[cfg(not(feature = "sync"))]
    /// let response = bucket.delete_objects(objects.clone()).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response = bucket.delete_objects(objects)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response = bucket.delete_objects_blocking(objects)?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_objects<I: Into<Vec<ObjectIdentifier>>>(
        &self,
        objects: I,
    ) -> Result<DeleteObjectsResult, S3Error> {
        let objects = objects.into();
        let mut result = DeleteObjectsResult {
            deleted: Vec::new(),
            errors: Vec::new(),
        };

        // Strip leading '/' from keys to match library convention.
        // Other methods (put_object, delete_object, etc.) strip the leading
        // slash when building the URL; we do the same for the XML body.
        let objects: Vec<ObjectIdentifier> = objects
            .into_iter()
            .map(|mut obj| {
                if let Some(stripped) = obj.key.strip_prefix('/') {
                    obj.key = stripped.to_string();
                }
                obj
            })
            .collect();

        for chunk in objects.chunks(1000) {
            let data = DeleteObjectsRequest {
                objects: chunk.to_vec(),
                quiet: false,
            };
            let command = Command::DeleteObjects { data };
            let request = RequestImpl::new(self, "/", command).await?;
            let response_data = request.response_data(false).await?;
            if response_data.status_code() >= 300 {
                return Err(error_from_response_data(response_data)?);
            }
            let msg: DeleteObjectsResult = quick_xml::de::from_str(response_data.as_str()?)?;
            result.deleted.extend(msg.deleted);
            result.errors.extend(msg.errors);
        }

        Ok(result)
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
    /// #[cfg(not(feature = "sync"))]
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
    pub async fn head_object<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<(HeadObjectResult, u16), S3Error> {
        let command = Command::HeadObject;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.put_object_with_content_type("/test.file", content, "text/plain").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.put_object_with_content_type("/test.file", content, "text/plain")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.put_object_with_content_type_blocking("/test.file", content, "text/plain")?;
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
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutObject {
            content,
            content_type,
            custom_headers: None,
            multipart: None,
        };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(true).await
    }

    /// Put into an S3 bucket, with explicit content-type and custom headers for the request.
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
    /// let mut headers = http::HeaderMap::new();
    /// headers.insert(
    ///     http::HeaderName::from_static("cache-control"),
    ///     "public, max-age=300".parse().unwrap(),
    /// );
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket
    ///     .put_object_with_content_type_and_headers("/test.file", content, "text/plain", Some(headers.clone())).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket
    ///     .put_object_with_content_type_and_headers("/test.file", content, "text/plain", Some(headers))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket
    ///     .put_object_with_content_type_and_headers_blocking("/test.file", content, "text/plain", Some(headers))?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_with_content_type_and_headers<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        content_type: &str,
        custom_headers: Option<HeaderMap>,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutObject {
            content,
            content_type,
            custom_headers,
            multipart: None,
        };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(true).await
    }

    /// Put into an S3 bucket, with custom headers for the request.
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
    /// let mut headers = http::HeaderMap::new();
    /// headers.insert(
    ///     http::HeaderName::from_static("cache-control"),
    ///     "public, max-age=300".parse().unwrap(),
    /// );
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.put_object_with_headers("/test.file", content, Some(headers.clone())).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.put_object_with_headers("/test.file", content, Some(headers))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.put_object_with_headers_blocking("/test.file", content, Some(headers))?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_with_headers<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        custom_headers: Option<HeaderMap>,
    ) -> Result<ResponseData, S3Error> {
        self.put_object_with_content_type_and_headers(
            path,
            content,
            "application/octet-stream",
            custom_headers,
        )
        .await
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.put_object("/test.file", content).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.put_object("/test.file", content)?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.put_object_blocking("/test.file", content)?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
    ) -> Result<ResponseData, S3Error> {
        self.put_object_with_content_type(path, content, "application/octet-stream")
            .await
    }

    /// Create a builder for PUT object operations with custom options
    ///
    /// This method returns a builder that allows configuring various options
    /// for the PUT operation including headers, content type, and metadata.
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
    /// let bucket = Bucket::new("my-bucket", "us-east-1".parse()?, Credentials::default()?)?;
    ///
    /// // Upload with custom headers using builder pattern
    /// #[cfg(not(feature = "sync"))]
    /// let response = bucket.put_object_builder("/my-file.txt", b"Hello, World!")
    ///     .with_content_type("text/plain")
    ///     .with_cache_control("public, max-age=3600")?
    ///     .with_metadata("author", "john-doe")?
    ///     .execute()
    ///     .await?;
    /// #[cfg(feature = "sync")]
    /// let response = bucket.put_object_builder("/my-file.txt", b"Hello, World!")
    ///     .with_content_type("text/plain")
    ///     .with_cache_control("public, max-age=3600")?
    ///     .with_metadata("author", "john-doe")?
    ///     .execute()
    ///     ?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_object_builder<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
    ) -> crate::put_object_request::PutObjectRequest<'_> {
        crate::put_object_request::PutObjectRequest::new(self, path, content)
    }

    fn _tags_xml<S: AsRef<str>>(&self, tags: &[(S, S)]) -> String {
        let mut s = String::new();
        let content = tags
            .iter()
            .map(|(name, value)| {
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")])?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.put_object_tagging_blocking("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")])?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn put_object_tagging<S: AsRef<str>>(
        &self,
        path: &str,
        tags: &[(S, S)],
    ) -> Result<ResponseData, S3Error> {
        let content = self._tags_xml(tags);
        let command = Command::PutObjectTagging { tags: &content };
        let request = RequestImpl::new(self, path, command).await?;
        request.response_data(false).await
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.delete_object_tagging("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.delete_object_tagging("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.delete_object_tagging_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn delete_object_tagging<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::DeleteObjectTagging;
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(false).await
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
    /// #[cfg(not(feature = "sync"))]
    /// let response_data = bucket.get_object_tagging("/test.file").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let response_data = bucket.get_object_tagging("/test.file")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let response_data = bucket.get_object_tagging_blocking("/test.file")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "tags")]
    #[maybe_async::maybe_async]
    pub async fn get_object_tagging<S: AsRef<str>>(
        &self,
        path: S,
    ) -> Result<(Vec<Tag>, u16), S3Error> {
        let command = Command::GetObjectTagging {};
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        let result = request.response_data(false).await?;

        let mut tags = Vec::new();

        if result.status_code() == 200 {
            let result_string = String::from_utf8_lossy(result.as_slice());

            // Add namespace if it doesn't exist
            let ns = "http://s3.amazonaws.com/doc/2006-03-01/";
            let result_string =
                if let Err(minidom::Error::MissingNamespace) = result_string.parse::<Element>() {
                    result_string
                        .replace("<Tagging>", &format!("<Tagging xmlns=\"{}\">", ns))
                        .into()
                } else {
                    result_string
                };

            if let Ok(tagging) = result_string.parse::<Element>() {
                for tag_set in tagging.children() {
                    if tag_set.is("TagSet", ns) {
                        for tag in tag_set.children() {
                            if tag.is("Tag", ns) {
                                let key = if let Some(element) = tag.get_child("Key", ns) {
                                    element.text()
                                } else {
                                    "Could not parse Key from Tag".to_string()
                                };
                                let value = if let Some(element) = tag.get_child("Value", ns) {
                                    element.text()
                                } else {
                                    "Could not parse Values from Tag".to_string()
                                };
                                tags.push(Tag { key, value });
                            }
                        }
                    }
                }
            }
        }

        Ok((tags, result.status_code()))
    }

    #[maybe_async::maybe_async]
    pub async fn list_page(
        &self,
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<usize>,
    ) -> Result<(ListBucketResult, u16), S3Error> {
        let command = if self.listobjects_v2 {
            Command::ListObjectsV2 {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys,
            }
        } else {
            // In the v1 ListObjects request, there is only one "marker"
            // field that serves as both the initial starting position,
            // and as the continuation token.
            Command::ListObjects {
                prefix,
                delimiter,
                marker: std::cmp::max(continuation_token, start_after),
                max_keys,
            }
        };
        let request = RequestImpl::new(self, "/", command).await?;
        let response_data = request.response_data(false).await?;
        let list_bucket_result = quick_xml::de::from_reader(response_data.as_slice())?;

        Ok((list_bucket_result, response_data.status_code()))
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
    /// #[cfg(not(feature = "sync"))]
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
    #[allow(clippy::assigning_clones)]
    pub async fn list(
        &self,
        prefix: String,
        delimiter: Option<String>,
    ) -> Result<Vec<ListBucketResult>, S3Error> {
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

    /// List one page of in-progress multipart uploads. Pass both returned cursor markers to
    /// continue a truncated listing; directory buckets and some compatible services may omit
    /// the upload ID marker.
    #[maybe_async::maybe_async]
    pub async fn list_multiparts_uploads_page(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        max_uploads: Option<usize>,
    ) -> Result<(ListMultipartUploadsResult, u16), S3Error> {
        let command = Command::ListMultipartUploads {
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
        };
        let request = RequestImpl::new(self, "/", command).await?;
        let response_data = request.response_data(false).await?;
        let list_bucket_result = quick_xml::de::from_reader(response_data.as_slice())?;

        Ok((list_bucket_result, response_data.status_code()))
    }

    /// List the ongoing multipart uploads of an S3 bucket. This may be useful to cleanup failed
    /// uploads, together with [`crate::bucket::Bucket::abort_upload`]. The method follows both
    /// pagination markers when supplied by the service and returns an error if a truncated
    /// response has no next key marker or repeats a cursor.
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
    /// #[cfg(not(feature = "sync"))]
    /// let results = bucket.list_multiparts_uploads(Some("/"), Some("/")).await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let results = bucket.list_multiparts_uploads(Some("/"), Some("/"))?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let results = bucket.list_multiparts_uploads_blocking(Some("/"), Some("/"))?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn list_multiparts_uploads(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Vec<ListMultipartUploadsResult>, S3Error> {
        let the_bucket = self.to_owned();
        let mut results = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        let mut seen_cursors = HashSet::new();

        loop {
            if !seen_cursors.insert((key_marker.clone(), upload_id_marker.clone())) {
                return Err(S3Error::InvalidMultipartUploadsPagination(
                    "pagination cursor repeated",
                ));
            }

            let (list_multiparts_uploads_result, _) = the_bucket
                .list_multiparts_uploads_page(prefix, delimiter, key_marker, upload_id_marker, None)
                .await?;

            if !list_multiparts_uploads_result.is_truncated {
                results.push(list_multiparts_uploads_result);
                break;
            }

            let next_key_marker = list_multiparts_uploads_result.next_marker.clone().ok_or(
                S3Error::InvalidMultipartUploadsPagination(
                    "truncated response has no next key marker",
                ),
            )?;
            let next_upload_id_marker =
                list_multiparts_uploads_result.next_upload_id_marker.clone();
            if seen_cursors
                .contains(&(Some(next_key_marker.clone()), next_upload_id_marker.clone()))
            {
                return Err(S3Error::InvalidMultipartUploadsPagination(
                    "pagination cursor repeated",
                ));
            }

            key_marker = Some(next_key_marker);
            upload_id_marker = next_upload_id_marker;
            results.push(list_multiparts_uploads_result);
        }

        Ok(results)
    }

    /// Abort a running multipart upload.
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
    /// #[cfg(not(feature = "sync"))]
    /// let results = bucket.abort_upload("/some/file.txt", "ZDFjM2I0YmEtMzU3ZC00OTQ1LTlkNGUtMTgxZThjYzIwNjA2").await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// let results = bucket.abort_upload("/some/file.txt", "ZDFjM2I0YmEtMzU3ZC00OTQ1LTlkNGUtMTgxZThjYzIwNjA2")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let results = bucket.abort_upload_blocking("/some/file.txt", "ZDFjM2I0YmEtMzU3ZC00OTQ1LTlkNGUtMTgxZThjYzIwNjA2")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), S3Error> {
        let abort = Command::AbortMultipartUpload { upload_id };
        let abort_request = RequestImpl::new(self, key, abort).await?;
        let response_data = abort_request.response_data(false).await?;

        if (200..300).contains(&response_data.status_code()) {
            Ok(())
        } else {
            let utf8_content = String::from_utf8(response_data.as_slice().to_vec())?;
            Err(S3Error::HttpFailWithBody(
                response_data.status_code(),
                utf8_content,
            ))
        }
    }

    /// Get path_style field of the Bucket struct
    pub fn is_path_style(&self) -> bool {
        self.path_style
    }

    /// Get negated path_style field of the Bucket struct
    pub fn is_subdomain_style(&self) -> bool {
        !self.path_style
    }

    /// Configure bucket to use path-style urls and headers
    pub fn set_path_style(&mut self) {
        self.path_style = true;
    }

    /// Configure bucket to use subdomain style urls and headers \[default\]
    pub fn set_subdomain_style(&mut self) {
        self.path_style = false;
    }

    /// Configure the total per-attempt timeout for HTTP requests, or disable
    /// the library-level deadline with `None`. Defaults to 60 seconds.
    ///
    /// Async backends apply this deadline from HTTP send through response-body
    /// reads used by buffered downloads, writer copies, and lazy streams.
    /// The synchronous backend uses its transport's timeout behavior; disabling
    /// the library-level deadline does not remove transport-specific limits.
    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.request_timeout = timeout;
    }

    /// Configure bucket to use the older ListObjects API
    ///
    /// If your provider doesn't support the ListObjectsV2 interface, set this to
    /// use the v1 ListObjects interface instead. This is currently needed at least
    /// for Google Cloud Storage.
    pub fn set_listobjects_v1(&mut self) {
        self.listobjects_v2 = false;
    }

    /// Configure bucket to use the newer ListObjectsV2 API
    pub fn set_listobjects_v2(&mut self) {
        self.listobjects_v2 = true;
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
    #[maybe_async::maybe_async]
    pub async fn access_key(&self) -> Result<Option<String>, S3Error> {
        Ok(self.credentials().await?.access_key)
    }

    /// Get a reference to the AWS secret key.
    #[maybe_async::maybe_async]
    pub async fn secret_key(&self) -> Result<Option<String>, S3Error> {
        Ok(self.credentials().await?.secret_key)
    }

    /// Get a reference to the AWS security token.
    #[maybe_async::maybe_async]
    pub async fn security_token(&self) -> Result<Option<String>, S3Error> {
        Ok(self.credentials().await?.security_token)
    }

    /// Get a reference to the AWS session token.
    #[maybe_async::maybe_async]
    pub async fn session_token(&self) -> Result<Option<String>, S3Error> {
        Ok(self.credentials().await?.session_token)
    }

    /// Get a reference to the full [`Credentials`](struct.Credentials.html)
    /// object used by this `Bucket`.
    #[maybe_async::async_impl]
    pub async fn credentials(&self) -> Result<Credentials, S3Error> {
        Ok(self.credentials.read().await.clone())
    }

    #[maybe_async::sync_impl]
    pub fn credentials(&self) -> Result<Credentials, S3Error> {
        match self.credentials.read() {
            Ok(credentials) => Ok(credentials.clone()),
            Err(_) => Err(S3Error::CredentialsReadLock),
        }
    }

    /// Change the credentials used by the Bucket.
    pub fn set_credentials(&mut self, credentials: Credentials) {
        self.credentials = Arc::new(RwLock::new(credentials));
        #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
        {
            self.credentials_refresh_gate = Arc::new(AsyncMutex::new(()));
        }
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
        self.extra_headers
            .insert(HeaderName::from_str(key).unwrap(), value.parse().unwrap());
    }

    /// Get a reference to the extra headers to be passed to the S3 API.
    pub fn extra_headers(&self) -> &HeaderMap {
        &self.extra_headers
    }

    /// Get a mutable reference to the extra headers to be passed to the S3
    /// API.
    pub fn extra_headers_mut(&mut self) -> &mut HeaderMap {
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

    pub fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }
}

#[cfg(test)]
mod test {

    use super::validate_success_xml_response;

    use crate::BucketConfiguration;
    use crate::Tag;
    use crate::creds::Credentials;
    use crate::error::S3Error;
    use crate::post_policy::{PostPolicyField, PostPolicyValue};
    use crate::region::Region;
    use crate::request::ResponseData;
    use crate::serde_types::{
        BucketLifecycleConfiguration, CorsConfiguration, CorsRule, Expiration, LifecycleFilter,
        LifecycleRule, Part,
    };
    use crate::{Bucket, PostPolicy};
    use http::header::{CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue};
    use std::collections::HashMap;
    use std::env;
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    use std::io::{Read, Write};
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    use std::net::TcpListener;
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    use std::sync::mpsc::{self, TryRecvError};
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    use std::thread;
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    use std::time::{Duration, Instant};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    #[test]
    fn concurrent_chunk_calculation_is_bounded_and_has_a_fallback() {
        let per_chunk = super::CHUNK_SIZE as u64 * 3;
        assert_eq!(Bucket::max_concurrent_chunks_for_memory(0), 3);
        assert_eq!(Bucket::max_concurrent_chunks_for_memory(per_chunk - 1), 2);
        assert_eq!(Bucket::max_concurrent_chunks_for_memory(per_chunk * 9), 9);
        assert_eq!(Bucket::max_concurrent_chunks_for_memory(per_chunk * 10), 10);
        assert_eq!(Bucket::max_concurrent_chunks_for_memory(u64::MAX), 10);
    }

    fn test_object_key(suffix: &str) -> String {
        format!(
            "{}{suffix}",
            env::var("RUST_S3_TEST_PREFIX").unwrap_or_default()
        )
    }

    fn test_object_path(suffix: &str) -> String {
        format!("/{}", test_object_key(suffix))
    }

    fn xml_response(body: &str) -> ResponseData {
        ResponseData::new(
            bytes::Bytes::copy_from_slice(body.as_bytes()),
            200,
            HashMap::new(),
        )
    }

    fn make_mock_stream_blocking(
        stream: std::net::TcpStream,
    ) -> std::io::Result<std::net::TcpStream> {
        // BSD accept can carry over O_NONBLOCK from the listener.
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    #[maybe_async::maybe_async]
    async fn run_multipart_upload_listing_case(
        responses: Vec<String>,
    ) -> (
        Result<Vec<crate::serde_types::ListMultipartUploadsResult>, S3Error>,
        Result<Vec<String>, String>,
    ) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        use std::thread;
        use std::time::{Duration, Instant};

        fn read_request_target(
            stream: &mut std::net::TcpStream,
            deadline: Instant,
        ) -> Result<String, String> {
            let mut headers = Vec::new();
            let mut byte = [0u8; 1];
            while !headers.ends_with(b"\r\n\r\n") {
                if headers.len() >= 16 * 1024 || Instant::now() >= deadline {
                    return Err("request headers exceeded the fixture bound".to_owned());
                }
                match stream.read(&mut byte) {
                    Ok(0) => return Err("client closed before sending request headers".to_owned()),
                    Ok(_) => headers.push(byte[0]),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(format!("failed to read request headers: {error}")),
                }
            }
            let request = String::from_utf8_lossy(&headers);
            let mut fields = request
                .lines()
                .next()
                .unwrap_or_default()
                .split_whitespace();
            let method = fields.next().unwrap_or_default();
            let target = fields.next().unwrap_or_default();
            if method != "GET" || target.is_empty() {
                return Err("unexpected request line in multipart listing fixture".to_owned());
            }
            Ok(target.to_owned())
        }

        fn send_listing_response(
            stream: &mut std::net::TcpStream,
            body: &str,
        ) -> std::io::Result<()> {
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (done_tx, done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut requests = Vec::new();
            for body in &responses {
                let (stream, _) = loop {
                    match listener.accept() {
                        Ok(connection) => break connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                return Err("timed out waiting for a pagination request".to_owned());
                            }
                            if done_rx.try_recv().is_ok() {
                                return Err(
                                    "client finished before all scripted requests".to_owned()
                                );
                            }
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => return Err(format!("failed to accept request: {error}")),
                    }
                };
                let mut stream = make_mock_stream_blocking(stream)
                    .map_err(|error| format!("failed to configure accepted stream: {error}"))?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .map_err(|error| format!("failed to set read timeout: {error}"))?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .map_err(|error| format!("failed to set write timeout: {error}"))?;
                let target = read_request_target(&mut stream, deadline)?;
                requests.push(target);
                send_listing_response(&mut stream, body)
                    .map_err(|error| format!("failed to send listing page: {error}"))?;
            }

            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let mut stream = make_mock_stream_blocking(stream).map_err(|error| {
                            format!("failed to configure extra stream: {error}")
                        })?;
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .map_err(|error| format!("failed to set read timeout: {error}"))?;
                        let target = read_request_target(&mut stream, deadline)?;
                        requests.push(target);
                        // Let an unexpected extra request complete so the client does not hang.
                        let final_page = "<ListMultipartUploadsResult><Bucket>test-bucket</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>";
                        send_listing_response(&mut stream, final_page)
                            .map_err(|error| format!("failed to answer extra request: {error}"))?;
                        return Err("client made more pagination requests than scripted".to_owned());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if done_rx.recv_timeout(Duration::from_millis(10)).is_ok() {
                            return Ok(requests);
                        }
                        if Instant::now() >= deadline {
                            return Err("client did not finish before fixture deadline".to_owned());
                        }
                    }
                    Err(error) => return Err(format!("failed to accept extra request: {error}")),
                }
            }
        });

        let credentials = Credentials::new(
            Some("test_access_key"),
            Some("test_secret_key"),
            None,
            None,
            None,
        )
        .unwrap();
        let bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style()
        .with_request_timeout(Duration::from_secs(4))
        .unwrap();
        let result = bucket.list_multiparts_uploads(Some("same-key"), None).await;
        let _ = done_tx.send(());
        let server_result = server.join().expect("multipart listing server panicked");
        (result, server_result)
    }

    fn multipart_listing_xml(
        next_key_marker: Option<&str>,
        next_upload_id_marker: Option<&str>,
        is_truncated: bool,
        upload: Option<(&str, &str)>,
    ) -> String {
        let mut xml = String::from("<ListMultipartUploadsResult><Bucket>test-bucket</Bucket>");
        if let Some(marker) = next_key_marker {
            xml.push_str(&format!("<NextKeyMarker>{marker}</NextKeyMarker>"));
        }
        if let Some(marker) = next_upload_id_marker {
            xml.push_str(&format!(
                "<NextUploadIdMarker>{marker}</NextUploadIdMarker>"
            ));
        }
        xml.push_str(&format!("<IsTruncated>{is_truncated}</IsTruncated>"));
        if let Some((key, upload_id)) = upload {
            xml.push_str(&format!(
                "<Upload><Initiated>2026-01-01T00:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass><Key>{key}</Key><UploadId>{upload_id}</UploadId></Upload>"
            ));
        }
        xml.push_str("</ListMultipartUploadsResult>");
        xml
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn multipart_upload_pagination_uses_both_markers_and_clears_missing_upload_marker() {
        fn query(target: &str) -> HashMap<String, String> {
            url::Url::parse(&format!("http://localhost{target}"))
                .unwrap()
                .query_pairs()
                .into_owned()
                .collect()
        }

        let (result, requests) = run_multipart_upload_listing_case(vec![
            multipart_listing_xml(
                Some("same-key"),
                Some("upload +/2"),
                true,
                Some(("same-key", "upload +/2")),
            ),
            multipart_listing_xml(None, None, false, Some(("same-key", "upload-2"))),
        ])
        .await;
        let pages = result.expect("two-page listing should succeed");
        let requests = requests.expect("mock server request sequence should succeed");
        assert_eq!(pages.len(), 2);
        let mut listed_ids = pages
            .iter()
            .flat_map(|page| page.uploads.iter().map(|upload| upload.id.clone()))
            .collect::<Vec<_>>();
        listed_ids.sort();
        assert_eq!(listed_ids, ["upload +/2", "upload-2"]);
        assert_eq!(requests.len(), 2);
        let second_query = query(&requests[1]);
        assert_eq!(second_query.get("key-marker").unwrap(), "same-key");
        assert_eq!(second_query.get("upload-id-marker").unwrap(), "upload +/2");

        let (result, requests) = run_multipart_upload_listing_case(vec![
            multipart_listing_xml(Some("same-key"), Some("id-one"), true, None),
            multipart_listing_xml(Some("later-key"), None, true, None),
            multipart_listing_xml(None, None, false, None),
        ])
        .await;
        assert_eq!(result.unwrap().len(), 3);
        let requests = requests.expect("key-only continuation should succeed");
        assert_eq!(requests.len(), 3);
        let second_query = query(&requests[1]);
        assert_eq!(second_query.get("upload-id-marker").unwrap(), "id-one");
        let third_query = query(&requests[2]);
        assert_eq!(third_query.get("key-marker").unwrap(), "later-key");
        assert!(!third_query.contains_key("upload-id-marker"));
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn multipart_upload_pagination_rejects_missing_repeated_and_cyclic_cursors() {
        let cases = vec![
            (
                "truncated response has no next key marker",
                vec![multipart_listing_xml(None, Some("id-a"), true, None)],
                1,
            ),
            (
                "pagination cursor repeated",
                vec![
                    multipart_listing_xml(Some("same-key"), Some("id-a"), true, None),
                    multipart_listing_xml(Some("same-key"), Some("id-a"), true, None),
                ],
                2,
            ),
            (
                "pagination cursor repeated",
                vec![
                    multipart_listing_xml(Some("same-key"), Some("id-a"), true, None),
                    multipart_listing_xml(Some("same-key"), Some("id-b"), true, None),
                    multipart_listing_xml(Some("same-key"), Some("id-a"), true, None),
                ],
                3,
            ),
        ];

        for (expected_reason, responses, expected_requests) in cases {
            let (result, requests) = run_multipart_upload_listing_case(responses).await;
            match result {
                Err(S3Error::InvalidMultipartUploadsPagination(reason)) => {
                    assert_eq!(reason, expected_reason);
                }
                other => panic!("expected pagination error, got {other:?}"),
            }
            let requests = requests.expect("mock server request sequence should succeed");
            assert_eq!(requests.len(), expected_requests);
        }
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    #[derive(Debug)]
    struct MultipartWireRequest {
        line: String,
        headers: HashMap<String, String>,
        content_length: usize,
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    #[maybe_async::maybe_async]
    async fn run_multipart_header_case(
        size: usize,
        fail_reader: bool,
        custom_headers: HeaderMap,
    ) -> (
        Result<(), S3Error>,
        Result<Vec<MultipartWireRequest>, String>,
    ) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        use std::thread;
        use std::time::{Duration, Instant};

        fn read_request(
            stream: &mut std::net::TcpStream,
            deadline: Instant,
        ) -> Result<MultipartWireRequest, String> {
            let mut raw_headers = Vec::new();
            let mut byte = [0u8; 1];
            while !raw_headers.ends_with(b"\r\n\r\n") {
                if raw_headers.len() >= 16 * 1024 || Instant::now() >= deadline {
                    return Err("header limit or deadline exceeded".to_owned());
                }
                stream
                    .read_exact(&mut byte)
                    .map_err(|error| format!("read headers: {error}"))?;
                raw_headers.push(byte[0]);
            }

            let header_text = String::from_utf8_lossy(&raw_headers);
            let mut lines = header_text.lines();
            let line = lines.next().unwrap_or_default().to_owned();
            let mut headers = HashMap::new();
            for header in lines {
                if let Some((name, value)) = header.split_once(':') {
                    let name = name.trim().to_ascii_lowercase();
                    if name != "authorization" && name != "x-amz-security-token" {
                        headers.insert(name, value.trim().to_owned());
                    }
                }
            }
            let content_length = headers
                .get("content-length")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            if content_length > super::CHUNK_SIZE + 16 * 1024 {
                return Err("request body exceeded test cap".to_owned());
            }
            let mut body = vec![0; content_length];
            stream
                .read_exact(&mut body)
                .map_err(|error| format!("read request body: {error}"))?;

            Ok(MultipartWireRequest {
                line,
                headers,
                content_length,
            })
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (done_tx, done_rx) = mpsc::channel();
        let rejected_headers = custom_headers.clone();
        let server = thread::spawn(move || {
            let expected = if rejected_headers.contains_key("content-length")
                || rejected_headers.contains_key("content-md5")
                || rejected_headers.contains_key("transfer-encoding")
                || rejected_headers.contains_key("x-amz-content-sha256")
                || rejected_headers.keys().any(|name| {
                    name.as_str().starts_with("x-amz-checksum-")
                        || name.as_str().starts_with("x-amz-sdk-checksum-")
                }) {
                Vec::new()
            } else if fail_reader {
                vec!["init", "abort"]
            } else if size < super::CHUNK_SIZE {
                vec!["put"]
            } else {
                let part_count = size.div_ceil(super::CHUNK_SIZE);
                let mut steps = vec!["init"];
                steps.extend(std::iter::repeat_n("part", part_count));
                steps.push("complete");
                steps
            };

            let deadline = Instant::now() + Duration::from_secs(12);
            let mut requests = Vec::new();
            for step in expected {
                let (mut stream, _) = loop {
                    if Instant::now() >= deadline {
                        return Err(format!("accept deadline after {} requests", requests.len()));
                    }
                    match listener.accept() {
                        Ok(pair) => break pair,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(format!("accept request: {error}")),
                    }
                };
                stream
                    .set_nonblocking(false)
                    .map_err(|error| error.to_string())?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(4)))
                    .map_err(|error| error.to_string())?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(4)))
                    .map_err(|error| error.to_string())?;
                let request = read_request(&mut stream, deadline)?;

                let (status, body, etag) = match step {
                    "init" => {
                        if !request.line.starts_with("POST ") || !request.line.contains("uploads") {
                            return Err(format!("unexpected initiation request: {}", request.line));
                        }
                        (
                            200,
                            "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>multipart-header-test</Key><UploadId>header-upload-id</UploadId></InitiateMultipartUploadResult>",
                            "",
                        )
                    }
                    "part" => {
                        if !request.line.starts_with("PUT ")
                            || !request.line.contains("partNumber=")
                        {
                            return Err(format!("unexpected part request: {}", request.line));
                        }
                        (200, "", "ETag: \"part-etag\"\r\n")
                    }
                    "complete" => {
                        if !request.line.starts_with("POST ") || !request.line.contains("uploadId=")
                        {
                            return Err(format!("unexpected completion request: {}", request.line));
                        }
                        (200, "<CompleteMultipartUploadResult/>", "")
                    }
                    "abort" => {
                        if !request.line.starts_with("DELETE ")
                            || !request.line.contains("uploadId=")
                        {
                            return Err(format!("unexpected abort request: {}", request.line));
                        }
                        (204, "", "")
                    }
                    _ => {
                        if !request.line.starts_with("PUT ") || request.line.contains('?') {
                            return Err(format!("unexpected small PUT request: {}", request.line));
                        }
                        (200, "", "ETag: \"small-etag\"\r\n")
                    }
                };

                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n{etag}Connection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .map_err(|error| format!("write response: {error}"))?;
                requests.push(request);
            }

            // The operation signals only after returning, so this also catches
            // an unexpected extra request without a timing-based sleep.
            loop {
                if done_rx.try_recv().is_ok() {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream
                                .set_nonblocking(false)
                                .map_err(|error| error.to_string())?;
                            stream
                                .set_read_timeout(Some(Duration::from_secs(4)))
                                .map_err(|error| error.to_string())?;
                            requests.push(read_request(&mut stream, deadline)?);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(error) => return Err(format!("final accept check: {error}")),
                    }
                    return Ok(requests);
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "operation completion deadline after {} requests",
                        requests.len()
                    ));
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .map_err(|error| error.to_string())?;
                        stream
                            .set_read_timeout(Some(Duration::from_secs(4)))
                            .map_err(|error| error.to_string())?;
                        let request = read_request(&mut stream, deadline)?;
                        requests.push(request);
                        return Ok(requests);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(format!("accept extra request: {error}")),
                }
            }
        });

        let credentials = Credentials::new(
            Some("test-access-key"),
            Some("test-secret-key"),
            None,
            None,
            None,
        )
        .unwrap();
        let mut bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style();
        bucket.add_header("x-amz-meta-uploader", "bucket-level-value");
        bucket.add_header("x-amz-request-payer", "owner-level-value");
        bucket.add_header("x-test-global-option", "raw-global-value");
        let mut reader = MultipartFaultReader {
            remaining: size,
            fail_at_eof: fail_reader,
        };
        let result = bucket
            .put_object_stream_builder("/multipart-header-test")
            .with_content_type("text/plain")
            .with_headers(custom_headers)
            .execute_stream(&mut reader)
            .await
            .map(|_| ());
        let _ = done_tx.send(());
        let server_result = server.join().expect("multipart header server panicked");
        (result, server_result)
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    fn multipart_test_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-amz-meta-uploader", "synthetic-test"),
            ("content-type", "application/x-custom-overridden"),
            ("cache-control", "public, max-age=120"),
            ("content-disposition", "attachment; filename=test.bin"),
            ("x-amz-storage-class", "STANDARD_IA"),
            ("x-amz-server-side-encryption", "aws:kms"),
            (
                "x-amz-server-side-encryption-aws-kms-key-id",
                "synthetic-kms-key",
            ),
            ("x-test-provider-option", "init-only"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                "synthetic-key-value",
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5",
                "synthetic-key-md5",
            ),
            ("x-amz-expected-bucket-owner", "123456789012"),
            ("x-amz-request-payer", "requester"),
            ("if-match", "\"existing-etag\""),
            ("if-none-match", "\"other-etag\""),
        ] {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    fn assert_wire_header(request: &MultipartWireRequest, name: &str, expected: Option<&str>) {
        assert_eq!(
            request.headers.get(name).map(String::as_str),
            expected,
            "{} {name}",
            request.line
        );
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    #[maybe_async::maybe_async]
    async fn assert_multipart_builder_header_routing() {
        let headers = multipart_test_headers();
        for size in [
            super::CHUNK_SIZE - 1,
            super::CHUNK_SIZE,
            super::CHUNK_SIZE + 1,
        ] {
            let (result, server_result) =
                run_multipart_header_case(size, false, headers.clone()).await;
            assert!(result.is_ok(), "stream upload failed: {result:?}");
            let requests = server_result.expect("local HTTP fixture failed");

            if size < super::CHUNK_SIZE {
                assert_eq!(requests.len(), 1);
                let put = &requests[0];
                assert!(put.line.starts_with("PUT "));
                for name in [
                    "x-amz-meta-uploader",
                    "cache-control",
                    "content-disposition",
                    "x-amz-storage-class",
                    "x-amz-server-side-encryption",
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    "x-test-provider-option",
                    "x-amz-server-side-encryption-customer-algorithm",
                    "x-amz-server-side-encryption-customer-key",
                    "x-amz-server-side-encryption-customer-key-md5",
                    "x-amz-expected-bucket-owner",
                    "x-amz-request-payer",
                    "if-match",
                    "if-none-match",
                ] {
                    assert_wire_header(put, name, Some(headers[name].to_str().unwrap()));
                }
                assert_wire_header(put, "x-test-global-option", Some("raw-global-value"));
                assert_wire_header(put, "content-type", Some("text/plain"));
                assert_wire_header(put, "content-length", Some(&size.to_string()));
                assert!(put.headers.contains_key("content-md5"));
                continue;
            }

            let initiate = requests
                .iter()
                .find(|request| request.line.contains("uploads"))
                .expect("multipart initiation request missing");
            assert_wire_header(initiate, "content-type", Some("text/plain"));
            assert_wire_header(initiate, "content-length", Some("0"));
            for name in [
                "x-amz-meta-uploader",
                "cache-control",
                "content-disposition",
                "x-amz-storage-class",
                "x-amz-server-side-encryption",
                "x-amz-server-side-encryption-aws-kms-key-id",
                "x-test-provider-option",
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5",
                "x-amz-expected-bucket-owner",
                "x-amz-request-payer",
            ] {
                assert_wire_header(initiate, name, Some(headers[name].to_str().unwrap()));
            }
            assert_wire_header(initiate, "x-test-global-option", Some("raw-global-value"));
            for name in ["if-match", "if-none-match"] {
                assert_wire_header(initiate, name, None);
            }

            let parts = requests
                .iter()
                .filter(|request| request.line.contains("partNumber="))
                .collect::<Vec<_>>();
            let mut part_numbers_and_lengths = parts
                .iter()
                .map(|request| {
                    let target = request.line.split_whitespace().nth(1).unwrap();
                    let query = target.split_once('?').unwrap().1;
                    let part_number = query
                        .split('&')
                        .find_map(|pair| pair.strip_prefix("partNumber="))
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    (part_number, request.content_length)
                })
                .collect::<Vec<_>>();
            part_numbers_and_lengths.sort_unstable_by_key(|(part_number, _)| *part_number);
            let expected_parts = if size == super::CHUNK_SIZE {
                vec![(1, super::CHUNK_SIZE)]
            } else {
                vec![(1, super::CHUNK_SIZE), (2, 1)]
            };
            assert_eq!(part_numbers_and_lengths, expected_parts);
            for part in parts {
                assert_wire_header(part, "content-type", Some("text/plain"));
                assert_wire_header(
                    part,
                    "content-length",
                    Some(&part.content_length.to_string()),
                );
                assert!(part.headers.contains_key("content-md5"));
                for name in [
                    "x-amz-server-side-encryption-customer-algorithm",
                    "x-amz-server-side-encryption-customer-key",
                    "x-amz-server-side-encryption-customer-key-md5",
                    "x-amz-expected-bucket-owner",
                    "x-amz-request-payer",
                ] {
                    assert_wire_header(part, name, Some(headers[name].to_str().unwrap()));
                }
                assert_wire_header(part, "x-test-global-option", Some("raw-global-value"));
                for name in [
                    "x-amz-meta-uploader",
                    "cache-control",
                    "content-disposition",
                    "x-amz-storage-class",
                    "x-amz-server-side-encryption",
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    "x-test-provider-option",
                    "if-match",
                    "if-none-match",
                ] {
                    assert_wire_header(part, name, None);
                }
            }

            let complete = requests
                .iter()
                .find(|request| {
                    request.line.starts_with("POST ") && request.line.contains("uploadId=")
                })
                .expect("multipart completion request missing");
            assert_wire_header(complete, "content-type", Some("application/xml"));
            assert!(complete.content_length > 0);
            for name in [
                "x-amz-server-side-encryption-customer-algorithm",
                "x-amz-server-side-encryption-customer-key",
                "x-amz-server-side-encryption-customer-key-md5",
                "x-amz-expected-bucket-owner",
                "x-amz-request-payer",
                "if-match",
                "if-none-match",
            ] {
                assert_wire_header(complete, name, Some(headers[name].to_str().unwrap()));
            }
            assert_wire_header(complete, "x-test-global-option", Some("raw-global-value"));
            for name in [
                "x-amz-meta-uploader",
                "cache-control",
                "content-disposition",
                "x-amz-storage-class",
                "x-amz-server-side-encryption",
                "x-amz-server-side-encryption-aws-kms-key-id",
                "x-test-provider-option",
            ] {
                assert_wire_header(complete, name, None);
            }
        }

        let (result, server_result) =
            run_multipart_header_case(super::CHUNK_SIZE, true, headers.clone()).await;
        assert!(matches!(
            result,
            Err(S3Error::Io(error)) if error.to_string() == "injected reader failure"
        ));
        let requests = server_result.expect("abort fixture failed");
        assert_eq!(requests.len(), 2);
        let abort = requests
            .iter()
            .find(|request| request.line.starts_with("DELETE "))
            .expect("multipart abort request missing");
        for name in ["x-amz-expected-bucket-owner", "x-amz-request-payer"] {
            assert_wire_header(abort, name, Some(headers[name].to_str().unwrap()));
        }
        assert_wire_header(abort, "x-test-global-option", Some("raw-global-value"));
        for name in [
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
            "x-amz-meta-uploader",
            "cache-control",
            "if-match",
            "if-none-match",
        ] {
            assert_wire_header(abort, name, None);
        }
        assert!(matches!(
            abort.headers.get("content-length").map(String::as_str),
            None | Some("0")
        ));
        assert!(matches!(
            abort.headers.get("content-type").map(String::as_str),
            None | Some("application/octet-stream")
        ));

        for rejected_name in [
            "content-md5",
            "content-length",
            "transfer-encoding",
            "x-amz-content-sha256",
            "x-amz-checksum-sha256",
            "x-amz-sdk-checksum-algorithm",
        ] {
            let mut rejected = HeaderMap::new();
            rejected.insert(
                HeaderName::from_bytes(rejected_name.as_bytes()).unwrap(),
                HeaderValue::from_static("synthetic-value"),
            );
            let (result, server_result) =
                run_multipart_header_case(super::CHUNK_SIZE, false, rejected).await;
            assert!(matches!(
                result,
                Err(S3Error::UnsupportedMultipartHeader(ref name)) if name.as_str() == rejected_name
            ));
            assert!(
                server_result.expect("rejection fixture failed").is_empty(),
                "rejected headers must fail before initiation"
            );
        }
    }

    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    #[tokio::test]
    async fn multipart_stream_builder_routes_headers_by_operation() {
        assert_multipart_builder_header_routing().await;
    }

    #[cfg(all(
        not(feature = "sync"),
        not(feature = "with-tokio"),
        feature = "with-async-std"
    ))]
    #[async_std::test]
    async fn multipart_stream_builder_routes_headers_by_operation() {
        assert_multipart_builder_header_routing().await;
    }

    #[allow(dead_code)]
    #[derive(Clone, Copy, Debug)]
    enum MultipartFailureCase {
        Reader,
        Part,
        Completion,
        CompletionEmbeddedError,
        Abort,
        SmallPut,
    }

    struct MultipartFaultReader {
        remaining: usize,
        fail_at_eof: bool,
    }

    impl MultipartFaultReader {
        fn read_into(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return if self.fail_at_eof {
                    Err(std::io::Error::other("injected reader failure"))
                } else {
                    Ok(0)
                };
            }
            let read = self.remaining.min(buffer.len());
            buffer[..read].fill(b'x');
            self.remaining -= read;
            Ok(read)
        }
    }

    impl std::io::Read for MultipartFaultReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.read_into(buffer)
        }
    }

    #[cfg(feature = "with-tokio")]
    impl tokio::io::AsyncRead for MultipartFaultReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            match this.read_into(buffer.initialize_unfilled()) {
                Ok(read) => {
                    buffer.advance(read);
                    std::task::Poll::Ready(Ok(()))
                }
                Err(error) => std::task::Poll::Ready(Err(error)),
            }
        }
    }

    #[cfg(feature = "with-async-std")]
    impl async_std::io::Read for MultipartFaultReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(self.get_mut().read_into(buffer))
        }
    }

    #[maybe_async::maybe_async]
    async fn run_multipart_failure_case(
        case: MultipartFailureCase,
    ) -> (Result<u16, S3Error>, Result<Vec<String>, String>) {
        use super::CHUNK_SIZE;
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        use std::thread;
        use std::time::{Duration, Instant};

        fn read_request(
            stream: &mut std::net::TcpStream,
            request_index: usize,
            deadline: Instant,
        ) -> Result<(String, usize), String> {
            let mut header_bytes = Vec::new();
            let mut byte = [0u8; 1];
            while !header_bytes.ends_with(b"\r\n\r\n") {
                if header_bytes.len() >= 16 * 1024 || Instant::now() >= deadline {
                    return Err(format!("header bounds exceeded at request {request_index}"));
                }
                stream
                    .read_exact(&mut byte)
                    .map_err(|error| format!("read headers at {request_index}: {error}"))?;
                header_bytes.push(byte[0]);
            }
            let header_text = String::from_utf8_lossy(&header_bytes);
            let request_line = header_text.lines().next().unwrap_or_default().to_owned();
            let content_length = header_text
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::to_owned)
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if content_length > CHUNK_SIZE + 64 * 1024 {
                return Err(format!("request body exceeded cap at {request_index}"));
            }
            let mut request_body = vec![0u8; content_length];
            stream
                .read_exact(&mut request_body)
                .map_err(|error| format!("read body at {request_index}: {error}"))?;
            Ok((request_line, content_length))
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (done_tx, done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let init_body = "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>multipart-test</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>";
            let part_failure = "part failure";
            let completion_failure = "completion failure";
            let completion_embedded_error =
                "<Error><Code>InternalError</Code><RequestId>completion-req</RequestId></Error>";
            let abort_failure = "abort failure";
            let put_failure = "fallback put failure";
            let failure_attempts = if cfg!(feature = "fail-on-err") {
                crate::get_retries() as usize + 1
            } else {
                1
            };
            let mut steps = vec![("POST ", "uploads", 200, init_body, false)];
            match case {
                MultipartFailureCase::Reader | MultipartFailureCase::Abort => {
                    if cfg!(feature = "sync") {
                        steps.push(("PUT ", "partNumber=1", 200, "", true));
                    }
                    let (status, body) = if matches!(case, MultipartFailureCase::Abort) {
                        (500, abort_failure)
                    } else {
                        (204, "")
                    };
                    // Cleanup is an ambiguous state-changing request and is never replayed.
                    steps.push(("DELETE ", "uploadId=upload-id", status, body, false));
                }
                MultipartFailureCase::SmallPut => {
                    steps.push(("DELETE ", "uploadId=upload-id", 204, "", false));
                    // Ordinary PUT is also not replayed after an ambiguous failure.
                    steps.push(("PUT ", "multipart-test", 500, put_failure, false));
                }
                MultipartFailureCase::Part => {
                    for _ in 0..failure_attempts {
                        steps.push(("PUT ", "partNumber=1", 500, part_failure, false));
                    }
                    steps.push(("DELETE ", "uploadId=upload-id", 204, "", false));
                }
                MultipartFailureCase::Completion
                | MultipartFailureCase::CompletionEmbeddedError => {
                    steps.push(("PUT ", "partNumber=1", 200, "", true));
                    if cfg!(feature = "sync") {
                        steps.push(("PUT ", "partNumber=2", 200, "", true));
                    }
                    let (status, body) = if matches!(case, MultipartFailureCase::Completion) {
                        (500, completion_failure)
                    } else {
                        (200, completion_embedded_error)
                    };
                    // Completion may have succeeded remotely even when its response failed.
                    steps.push(("POST ", "uploadId=upload-id", status, body, false));
                    steps.push(("DELETE ", "uploadId=upload-id", 204, "", false));
                }
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut requests = Vec::new();

            for (expected_method, expected_query, status, body, with_etag) in steps {
                let (mut stream, _) = loop {
                    if Instant::now() >= deadline {
                        return Err(format!("accept deadline after {} requests", requests.len()));
                    }
                    match listener.accept() {
                        Ok(pair) => break pair,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(format!("accept failed: {error}")),
                    }
                };
                stream
                    .set_nonblocking(false)
                    .map_err(|error| format!("set blocking: {error}"))?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .map_err(|error| format!("set read timeout: {error}"))?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .map_err(|error| format!("set write timeout: {error}"))?;
                let (request_line, _) = read_request(&mut stream, requests.len() + 1, deadline)?;
                if !request_line.starts_with(expected_method)
                    || !request_line.contains(expected_query)
                {
                    return Err(format!(
                        "unexpected request {}: method/query mismatch",
                        requests.len() + 1
                    ));
                }
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "Internal Server Error"
                };
                let etag = if with_etag {
                    "ETag: \"part-etag\"\r\n"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\n{etag}Connection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).map_err(|error| {
                    format!("write response at {}: {error}", requests.len() + 1)
                })?;
                requests.push(request_line);
            }

            // Keep accepting until the client signals its call has returned;
            // this detects an unexpected duplicate abort without a fixed sleep.
            loop {
                if done_rx.try_recv().is_ok() {
                    return Ok(requests);
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "post-request deadline after {} requests",
                        requests.len()
                    ));
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .map_err(|error| format!("set blocking for extra request: {error}"))?;
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .map_err(|error| format!("set extra read timeout: {error}"))?;
                        stream
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .map_err(|error| format!("set extra write timeout: {error}"))?;
                        let (request_line, _) =
                            read_request(&mut stream, requests.len() + 1, deadline)?;
                        requests.push(format!("unexpected extra request: {request_line}"));
                        stream
                            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .map_err(|error| format!("write extra response: {error}"))?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(format!("accept extra request failed: {error}")),
                }
            }
        });

        let credentials = Credentials::new(
            Some("test_access_key"),
            Some("test_secret_key"),
            None,
            None,
            None,
        )
        .unwrap();
        let bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style()
        .with_request_timeout(Duration::from_secs(4))
        .unwrap();
        let mut reader = MultipartFaultReader {
            remaining: if matches!(case, MultipartFailureCase::SmallPut) {
                1
            } else {
                CHUNK_SIZE
            },
            fail_at_eof: matches!(
                case,
                MultipartFailureCase::Reader | MultipartFailureCase::Abort
            ),
        };
        let result = bucket
            .put_object_stream_with_content_type(
                &mut reader,
                "/multipart-test",
                "application/octet-stream",
            )
            .await;
        #[cfg(not(feature = "sync"))]
        let result = result.map(|response| response.status_code());
        let _ = done_tx.send(());
        let server_result = server.join().expect("mock multipart server panicked");
        (result, server_result)
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn multipart_stream_errors_attempt_abort_and_preserve_primary_error() {
        let failure_attempts = if cfg!(feature = "fail-on-err") {
            crate::get_retries() as usize + 1
        } else {
            1
        };
        for case in [
            MultipartFailureCase::Reader,
            MultipartFailureCase::Part,
            MultipartFailureCase::Completion,
            MultipartFailureCase::CompletionEmbeddedError,
            MultipartFailureCase::Abort,
            #[cfg(feature = "sync")]
            MultipartFailureCase::SmallPut,
        ] {
            let (result, requests) = run_multipart_failure_case(case).await;
            let requests = requests.expect("mock server request sequence failed");
            let expected_requests = match case {
                MultipartFailureCase::Reader => 2 + usize::from(cfg!(feature = "sync")),
                MultipartFailureCase::Part => 2 + failure_attempts,
                MultipartFailureCase::Completion => 4 + usize::from(cfg!(feature = "sync")),
                MultipartFailureCase::CompletionEmbeddedError => {
                    4 + usize::from(cfg!(feature = "sync"))
                }
                MultipartFailureCase::Abort => 2 + usize::from(cfg!(feature = "sync")),
                MultipartFailureCase::SmallPut => 3,
            };
            assert_eq!(requests.len(), expected_requests);
            if !matches!(case, MultipartFailureCase::SmallPut) {
                assert!(result.is_err(), "{case:?} should return an error");
            }

            match case {
                MultipartFailureCase::Reader | MultipartFailureCase::Abort => {
                    assert!(
                        matches!(result, Err(S3Error::Io(error)) if error.to_string() == "injected reader failure")
                    );
                }
                MultipartFailureCase::Part => {
                    assert!(matches!(
                        result,
                        Err(S3Error::HttpFail | S3Error::HttpFailWithBody(500, _))
                    ));
                }
                MultipartFailureCase::Completion => match result {
                    Err(S3Error::HttpFailWithBody(500, body)) => {
                        assert_eq!(body, "completion failure");
                    }
                    Err(S3Error::HttpFail) if cfg!(feature = "fail-on-err") => {}
                    other => panic!("completion failure was not preserved: {other:?}"),
                },
                MultipartFailureCase::CompletionEmbeddedError => {
                    assert!(
                        matches!(result, Err(S3Error::HttpFailWithBody(200, body)) if body.contains("completion-req"))
                    );
                }
                MultipartFailureCase::SmallPut => {
                    if cfg!(feature = "fail-on-err") {
                        assert!(matches!(
                            result,
                            Err(S3Error::HttpFail | S3Error::HttpFailWithBody(500, _))
                        ));
                    } else {
                        assert!(matches!(result, Ok(500)));
                    }
                }
            }
        }
    }

    #[test]
    fn accepted_mock_stream_blocks_until_delayed_client_byte() {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::mpsc;
        use std::thread;
        use std::time::{Duration, Instant};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(0);
        let (read_result_tx, read_result_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            let (stream, _) = loop {
                match listener.accept() {
                    Ok(pair) => break pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "mock accept timed out");
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock accept failed: {error}"),
                }
            };
            let mut stream = make_mock_stream_blocking(stream).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            accepted_tx.send(()).unwrap();
            let mut byte = [0];
            read_result_tx
                .send(stream.read_exact(&mut byte).map(|()| byte[0]))
                .unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        accepted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server did not accept the client");
        assert!(matches!(
            read_result_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        client.write_all(b"x").unwrap();
        assert_eq!(
            read_result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("server did not finish reading")
                .unwrap(),
            b'x'
        );
        server.join().unwrap();
    }

    #[test]
    fn xml_response_embedded_error_rejects_invalid_or_unexpected_documents() {
        for body in [
            "",
            "<CopyObjectResult>",
            "<CopyObjectResult></CopyObjectResult><Extra/>",
            "<CopyObjectResult></CopyObjectResult>trailing",
            "<CopyObjectResult/>&amp;",
            "<CopyObjectResult><A></CopyObjectResult>",
            "<CopyObjectResult A=\"unterminated></CopyObjectResult>",
            "<CopyObjectResult>&unknown;</CopyObjectResult>",
            "<?xml version=\"1.0\"?><?xml version=\"1.0\"?><CopyObjectResult/>",
            "<?xml bogus?><CopyObjectResult/>",
            "<?xml version=\"1.0\"?><CopyObjectResult><?xml version=\"1.0\"?></CopyObjectResult>",
            "<CopyObjectResult><!-- invalid -- comment --></CopyObjectResult>",
        ] {
            assert!(
                matches!(
                    validate_success_xml_response(xml_response(body), "CopyObjectResult"),
                    Err(S3Error::SerdeXml(_))
                ),
                "accepted invalid XML: {body:?}"
            );
        }
        assert!(matches!(
            validate_success_xml_response(xml_response("<Other/>"), "CopyObjectResult"),
            Err(S3Error::HttpFailWithBody(200, _))
        ));
    }

    #[test]
    fn xml_response_embedded_error_accepts_xml_11_text_and_decoded_attributes() {
        let body = "<?xml version=\"1.1\"?>\n<CopyObjectResult note=\"café&amp;tea\">first\r\u{0085}second</CopyObjectResult>\n";
        assert!(validate_success_xml_response(xml_response(body), "CopyObjectResult").is_ok());
    }

    #[test]
    fn bucket_debug_redacts_nested_credentials() {
        let credentials = Credentials::new(
            Some("BUCKET_ACCESS_KEY_SENTINEL"),
            Some("BUCKET_SECRET_KEY_SENTINEL"),
            Some("BUCKET_SECURITY_TOKEN_SENTINEL"),
            Some("BUCKET_SESSION_TOKEN_SENTINEL"),
            None,
        )
        .unwrap();
        let bucket = Bucket::new(
            "debug-test-bucket",
            "us-east-1".parse::<Region>().unwrap(),
            credentials,
        )
        .unwrap();

        for rendered in [format!("{bucket:?}"), format!("{bucket:#?}")] {
            for sentinel in [
                "BUCKET_ACCESS_KEY_SENTINEL",
                "BUCKET_SECRET_KEY_SENTINEL",
                "BUCKET_SECURITY_TOKEN_SENTINEL",
                "BUCKET_SESSION_TOKEN_SENTINEL",
            ] {
                assert!(
                    !rendered.contains(sentinel),
                    "bucket debug output leaked a credential"
                );
            }
        }
    }

    #[test]
    fn xml_response_embedded_error_preserves_success_framing_and_service_error_body() {
        let success = xml_response(
            "<s:CopyObjectResult xmlns:s=\"urn:s3\"><s:ETag>\"abc\"</s:ETag><Extra/></s:CopyObjectResult>",
        );
        assert!(validate_success_xml_response(success, "CopyObjectResult").is_ok());

        let error_body = "<Error><Code>SlowDown</Code><RequestId>req-123</RequestId></Error>";
        assert!(matches!(
            validate_success_xml_response(xml_response(error_body), "CopyObjectResult"),
            Err(S3Error::HttpFailWithBody(200, body)) if body == error_body
        ));

        let non_success =
            ResponseData::new(bytes::Bytes::from_static(b"not xml"), 404, HashMap::new());
        assert_eq!(
            validate_success_xml_response(non_success, "CopyObjectResult")
                .unwrap()
                .status_code(),
            404
        );
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn xml_response_embedded_error_is_checked_by_copy_and_multipart_operations() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;
        use std::time::{Duration, Instant};

        fn server_error(
            stage: &str,
            completed_requests: usize,
            error: &dyn std::fmt::Display,
        ) -> String {
            format!(
                "stage={stage} completed_requests={completed_requests} request_index={} error={error}",
                completed_requests + 1
            )
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let responses = [
                "<CopyObjectResult><ETag>\"copy-etag\"</ETag></CopyObjectResult>",
                " \n<?xml version=\"1.0\"?>\n<CompleteMultipartUploadResult> \n<ETag>\"multipart-etag\"</ETag></CompleteMultipartUploadResult> \n",
                "<Error><Code>SlowDown</Code><RequestId>copy-req</RequestId></Error>",
                " \n<?xml version=\"1.0\"?>\n<Error><Code>InternalError</Code><RequestId>multipart-req</RequestId></Error> \n",
                "<CopyObjectResult>",
            ];
            let mut count = 0;
            while count < responses.len() {
                // This test makes five sequential requests, each with its own
                // client timeout. Bound each accept separately so time spent
                // on earlier calls does not consume later calls' window.
                let request_deadline = Instant::now() + Duration::from_secs(8);
                let (stream, _) = loop {
                    match listener.accept() {
                        Ok(pair) => break pair,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() < request_deadline {
                                thread::sleep(Duration::from_millis(5));
                                continue;
                            }
                            return Err(format!(
                                "stage=accept_deadline completed_requests={count} expected_requests={} deadline_elapsed=true",
                                responses.len()
                            ));
                        }
                        Err(error) => {
                            return Err(format!(
                                "stage=accept completed_requests={count} next_request={} error_kind={:?} error={error}",
                                count + 1,
                                error.kind()
                            ));
                        }
                    }
                };
                let mut request = Vec::new();
                let mut stream = make_mock_stream_blocking(stream)
                    .map_err(|error| server_error("set_blocking", count, &error))?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| server_error("set_read_timeout", count, &error))?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| server_error("set_write_timeout", count, &error))?;
                let mut byte = [0u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    if request.len() >= 16 * 1024 || Instant::now() >= request_deadline {
                        return Err(server_error(
                            "read_headers_bounds",
                            count,
                            &"request headers exceeded bounds",
                        ));
                    }
                    stream
                        .read_exact(&mut byte)
                        .map_err(|error| server_error("read_headers", count, &error))?;
                    request.push(byte[0]);
                }
                let headers = String::from_utf8_lossy(&request).to_ascii_lowercase();
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let content_length_value = content_length;
                if content_length_value > 64 * 1024 {
                    return Err(server_error(
                        "validate_content_length",
                        count,
                        &"request body exceeded mock server limit",
                    ));
                }
                let mut body = vec![0; content_length_value];
                stream
                    .read_exact(&mut body)
                    .map_err(|error| server_error("read_body", count, &error))?;
                let response_body = responses[count];
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream
                    .write_all(response.as_bytes())
                    .map_err(|error| server_error("write_response", count, &error))?;
                count += 1;
            }
            Ok(count)
        });

        let credentials = Credentials::new(
            Some("test_access_key"),
            Some("test_secret_key"),
            None,
            None,
            None,
        )
        .unwrap();
        let bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style()
        .with_request_timeout(Duration::from_secs(4))
        .unwrap();

        let copy_ok = bucket.copy_object_internal("/source", "/destination").await;
        let multipart_ok = bucket
            .complete_multipart_upload(
                "/destination",
                "upload-id",
                vec![Part {
                    etag: "\"part-etag\"".to_owned(),
                    part_number: 1,
                }],
            )
            .await;
        let copy_error = bucket.copy_object_internal("/source", "/destination").await;
        let multipart_error = bucket
            .complete_multipart_upload(
                "/destination",
                "upload-id",
                vec![Part {
                    etag: "\"part-etag\"".to_owned(),
                    part_number: 1,
                }],
            )
            .await;
        let malformed_copy = bucket.copy_object_internal("/source", "/destination").await;

        let server_result = server.join().expect("mock server panicked");
        assert_eq!(server_result.unwrap(), 5);
        assert_eq!(copy_ok.unwrap(), 200);
        assert_eq!(multipart_ok.unwrap().status_code(), 200);
        assert!(
            matches!(copy_error, Err(S3Error::HttpFailWithBody(200, body)) if body.contains("SlowDown") && body.contains("copy-req"))
        );
        assert!(
            matches!(multipart_error, Err(S3Error::HttpFailWithBody(200, body)) if body.contains("InternalError") && body.contains("multipart-req"))
        );
        assert!(matches!(malformed_copy, Err(S3Error::SerdeXml(_))));
    }

    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    #[tokio::test]
    async fn test_object_exists_404_does_not_retry() {
        init();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let credentials = Credentials::new(
            Some("test_access_key"),
            Some("test_secret_key"),
            None,
            None,
            None,
        )
        .unwrap();
        let bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style();
        listener.set_nonblocking(true).unwrap();
        let (stop_server, stop_rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let mut requests = 0;
            let accept_deadline = Instant::now() + Duration::from_secs(15);
            loop {
                match stop_rx.try_recv() {
                    Ok(()) | Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => {}
                }
                assert!(
                    Instant::now() < accept_deadline,
                    "mock server did not receive a stop signal before its deadline"
                );

                match listener.accept() {
                    Ok((stream, _)) => {
                        requests += 1;
                        let mut stream = make_mock_stream_blocking(stream).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .unwrap();

                        let mut request = Vec::new();
                        let mut byte = [0; 1];
                        let header_deadline = Instant::now() + Duration::from_secs(3);
                        while !request.ends_with(b"\r\n\r\n") {
                            assert!(
                                request.len() < 16 * 1024,
                                "mock server received oversized HTTP headers"
                            );
                            assert!(
                                Instant::now() < header_deadline,
                                "mock server timed out reading HTTP headers"
                            );
                            match stream.read(&mut byte) {
                                Ok(0) => panic!("client closed before sending HTTP headers"),
                                Ok(_) => request.push(byte[0]),
                                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                                    continue;
                                }
                                Err(error) => panic!("failed to read HTTP request: {error}"),
                            }
                        }
                        stream.write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= accept_deadline {
                            break;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
            requests
        });

        let exists =
            tokio::time::timeout(Duration::from_secs(5), bucket.object_exists("/missing.txt"))
                .await;
        let _ = stop_server.send(());
        let request_count = server.join().unwrap();

        assert!(!exists.expect("object_exists request timed out").unwrap());
        assert_eq!(request_count, 1);
    }

    #[test]
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    #[allow(deprecated)]
    fn dangerous_config_correct_spelling_and_compat_alias_set_same_options() {
        let bucket = Bucket::new(
            "test-bucket",
            Region::Custom {
                region: "test-region".to_owned(),
                endpoint: "https://example.com".to_owned(),
            },
            Credentials::anonymous().unwrap(),
        )
        .unwrap();

        let corrected = bucket.set_dangerous_config(true, true).unwrap();
        let deprecated_alias = bucket.set_dangereous_config(true, true).unwrap();

        assert!(corrected.client_options.accept_invalid_certs);
        assert!(corrected.client_options.accept_invalid_hostnames);
        assert_eq!(
            corrected.client_options.accept_invalid_certs,
            deprecated_alias.client_options.accept_invalid_certs
        );
        assert_eq!(
            corrected.client_options.accept_invalid_hostnames,
            deprecated_alias.client_options.accept_invalid_hostnames
        );
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

    fn test_minio_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("MINIO_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("MINIO_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_digital_ocean_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("DIGITAL_OCEAN_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("DIGITAL_OCEAN_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_r2_credentials() -> Credentials {
        Credentials::new(
            Some(&env::var("R2_ACCESS_KEY_ID").unwrap()),
            Some(&env::var("R2_SECRET_ACCESS_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn test_aws_bucket() -> Box<Bucket> {
        Bucket::new(
            "rust-s3-test",
            "eu-central-1".parse().unwrap(),
            test_aws_credentials(),
        )
        .unwrap()
    }

    fn test_wasabi_bucket() -> Box<Bucket> {
        Bucket::new(
            "rust-s3",
            "wa-eu-central-1".parse().unwrap(),
            test_wasabi_credentials(),
        )
        .unwrap()
    }

    fn test_gc_bucket() -> Box<Bucket> {
        let mut bucket = Bucket::new(
            "rust-s3",
            Region::Custom {
                region: "us-east1".to_owned(),
                endpoint: "https://storage.googleapis.com".to_owned(),
            },
            test_gc_credentials(),
        )
        .unwrap();
        bucket.set_listobjects_v1();
        bucket
    }

    fn test_minio_bucket() -> Box<Bucket> {
        Bucket::new(
            "rust-s3",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint: "http://localhost:9000".to_owned(),
            },
            test_minio_credentials(),
        )
        .unwrap()
        .with_path_style()
    }

    /// Bucket with hardcoded fake credentials for tests that only exercise
    /// local signing logic and never hit the network.
    fn test_presign_bucket() -> Box<Bucket> {
        Bucket::new(
            "rust-s3",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint: "http://localhost:9000".to_owned(),
            },
            Credentials::new(
                Some("test_access_key"),
                Some("test_secret_key"),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .with_path_style()
    }

    #[allow(dead_code)]
    fn test_digital_ocean_bucket() -> Box<Bucket> {
        Bucket::new("rust-s3", Region::DoFra1, test_digital_ocean_credentials()).unwrap()
    }

    fn test_r2_bucket() -> Box<Bucket> {
        Bucket::new(
            "rust-s3",
            Region::R2 {
                account_id: "f048f3132be36fa1aaa8611992002b3f".to_string(),
            },
            test_r2_credentials(),
        )
        .unwrap()
    }

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    #[maybe_async::maybe_async]
    async fn put_head_get_delete_object(bucket: Bucket, head: bool) {
        let s3_path = test_object_path("+test.file");
        let non_existant_path = test_object_path("+non_existant.file");
        let test: Vec<u8> = object(3072);

        let response_data = bucket.put_object(&s3_path, &test).await.unwrap();
        assert_eq!(response_data.status_code(), 200);

        // let attributes = bucket
        //     .get_object_attributes(s3_path, "904662384344", None)
        //     .await
        //     .unwrap();

        let response_data = bucket.get_object(&s3_path).await.unwrap();
        assert_eq!(response_data.status_code(), 200);
        assert_eq!(test, response_data.as_slice());

        let exists = bucket.object_exists(&s3_path).await.unwrap();
        assert!(exists);

        let not_exists = bucket.object_exists(&non_existant_path).await.unwrap();
        assert!(!not_exists);

        let response_data = bucket
            .get_object_range(&s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(test[100..1001].to_vec(), response_data.as_slice());

        // Test single-byte range read (start == end)
        let response_data = bucket
            .get_object_range(&s3_path, 100, Some(100))
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(vec![test[100]], response_data.as_slice());

        if head {
            let (_head_object_result, code) = bucket.head_object(&s3_path).await.unwrap();
            // println!("{:?}", head_object_result);
            assert_eq!(code, 200);
        }

        // println!("{:?}", head_object_result);
        let response_data = bucket.delete_object(&s3_path).await.unwrap();
        assert_eq!(response_data.status_code(), 204);
    }

    #[maybe_async::maybe_async]
    async fn put_head_delete_object_with_headers(bucket: Bucket) {
        let s3_path = test_object_path("+test.file");
        let non_existant_path = test_object_path("+non_existant.file");
        let test: Vec<u8> = object(3072);
        let header_value = "max-age=42";

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(CACHE_CONTROL, HeaderValue::from_static(header_value));
        custom_headers.insert(
            HeaderName::from_static("test-key"),
            "value".parse().unwrap(),
        );

        let response_data = bucket
            .put_object_with_headers(&s3_path, &test, Some(custom_headers.clone()))
            .await
            .expect("Put object with custom headers failed");
        assert_eq!(response_data.status_code(), 200);

        let response_data = bucket.get_object(&s3_path).await.unwrap();
        assert_eq!(response_data.status_code(), 200);
        assert_eq!(test, response_data.as_slice());

        let exists = bucket.object_exists(&s3_path).await.unwrap();
        assert!(exists);

        let not_exists = bucket.object_exists(&non_existant_path).await.unwrap();
        assert!(!not_exists);

        let response_data = bucket
            .get_object_range(&s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(test[100..1001].to_vec(), response_data.as_slice());

        let (head_object_result, code) = bucket.head_object(&s3_path).await.unwrap();
        // println!("{:?}", head_object_result);
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.cache_control,
            Some(header_value.to_string())
        );

        let response_data = bucket.delete_object(&s3_path).await.unwrap();
        assert_eq!(response_data.status_code(), 204);
    }

    #[ignore]
    #[cfg(feature = "tags")]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_tagging_aws() {
        let bucket = test_aws_bucket();
        let tagging_path = test_object_key("tagging_test");
        let target_tags = vec![
            Tag {
                key: "Tag1".to_string(),
                value: "Value1".to_string(),
            },
            Tag {
                key: "Tag2".to_string(),
                value: "Value2".to_string(),
            },
        ];
        let empty_tags: Vec<Tag> = Vec::new();
        let response_data = bucket
            .put_object(&tagging_path, b"Gimme tags")
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (tags, _code) = bucket.get_object_tagging(&tagging_path).await.unwrap();
        assert_eq!(tags, empty_tags);
        let response_data = bucket
            .put_object_tagging(&tagging_path, &[("Tag1", "Value1"), ("Tag2", "Value2")])
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (mut tags, _code) = bucket.get_object_tagging(&tagging_path).await.unwrap();
        tags.sort_by(|left, right| left.key.cmp(&right.key));
        let mut target_tags = target_tags;
        target_tags.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(tags, target_tags);
        let _response_data = bucket.delete_object(&tagging_path).await.unwrap();
    }

    #[ignore]
    #[cfg(feature = "tags")]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_tagging_minio() {
        let bucket = test_minio_bucket();
        let tagging_path = test_object_key("tagging_test");
        let target_tags = vec![
            Tag {
                key: "Tag1".to_string(),
                value: "Value1".to_string(),
            },
            Tag {
                key: "Tag2".to_string(),
                value: "Value2".to_string(),
            },
        ];
        let empty_tags: Vec<Tag> = Vec::new();
        let response_data = bucket
            .put_object(&tagging_path, b"Gimme tags")
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (tags, _code) = bucket.get_object_tagging(&tagging_path).await.unwrap();
        assert_eq!(tags, empty_tags);
        let response_data = bucket
            .put_object_tagging(&tagging_path, &[("Tag1", "Value1"), ("Tag2", "Value2")])
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (mut tags, _code) = bucket.get_object_tagging(&tagging_path).await.unwrap();
        tags.sort_by(|left, right| left.key.cmp(&right.key));
        let mut target_tags = target_tags;
        target_tags.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(tags, target_tags);
        let _response_data = bucket.delete_object(&tagging_path).await.unwrap();
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
    async fn streaming_big_aws_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_aws_bucket()).await;
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
    async fn streaming_big_gc_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_gc_bucket()).await;
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
    async fn streaming_big_minio_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_minio_bucket()).await;
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
    async fn streaming_big_wasabi_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_wasabi_bucket()).await;
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
    async fn streaming_big_digital_ocean_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_digital_ocean_bucket()).await;
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
    async fn streaming_big_r2_put_head_get_delete_object() {
        streaming_test_put_get_delete_big_object(*test_r2_bucket()).await;
    }

    // Test multi-part upload
    #[maybe_async::maybe_async]
    async fn streaming_test_put_get_delete_big_object(bucket: Bucket) {
        #[cfg(feature = "with-async-std")]
        use async_std::fs::File;
        #[cfg(feature = "with-async-std")]
        use async_std::io::WriteExt;
        #[cfg(feature = "with-async-std")]
        use async_std::stream::StreamExt;
        #[cfg(feature = "with-tokio")]
        use futures_util::StreamExt;
        #[cfg(not(any(feature = "with-tokio", feature = "with-async-std")))]
        use std::fs::File;
        #[cfg(not(any(feature = "with-tokio", feature = "with-async-std")))]
        use std::io::Write;
        #[cfg(feature = "with-tokio")]
        use tokio::fs::File;
        #[cfg(feature = "with-tokio")]
        use tokio::io::AsyncWriteExt;

        init();
        let remote_path = test_object_key("+stream_test_big");
        let local_path = "+stream_test_big";
        std::fs::remove_file(local_path).unwrap_or(());
        let content: Vec<u8> = (0..20_000_000).map(|i| (i % 251) as u8).collect();

        let mut file = File::create(local_path).await.unwrap();
        file.write_all(&content).await.unwrap();
        file.flush().await.unwrap();
        let mut reader = File::open(local_path).await.unwrap();

        #[cfg(not(feature = "sync"))]
        let response = bucket
            .put_object_stream_builder(&remote_path)
            .with_content_type("application/x-rust-s3-stream-test")
            .with_headers(streaming_builder_test_headers())
            .execute_stream(&mut reader)
            .await
            .unwrap();
        #[cfg(feature = "sync")]
        let response = bucket
            .put_object_stream(&mut reader, &remote_path)
            .await
            .unwrap();
        #[cfg(not(feature = "sync"))]
        assert_eq!(response.status_code(), 200);
        #[cfg(feature = "sync")]
        assert_eq!(response, 200);
        #[cfg(not(feature = "sync"))]
        {
            let (head, code) = bucket.head_object(&remote_path).await.unwrap();
            assert_eq!(code, 200);
            assert_eq!(
                head.content_type.as_deref(),
                Some("application/x-rust-s3-stream-test")
            );
            assert_eq!(head.cache_control.as_deref(), Some("public, max-age=120"));
            assert_eq!(
                head.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("stream-marker"))
                    .map(String::as_str),
                Some("multipart-builder")
            );
        }
        let mut writer = Vec::new();
        let code = bucket
            .get_object_to_writer(&remote_path, &mut writer)
            .await
            .unwrap();
        assert_eq!(code, 200);
        assert!(
            content == writer,
            "writer download differs from uploaded bytes"
        );
        assert_eq!(content.len(), 20_000_000);

        #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
        {
            let mut response_data_stream = bucket.get_object_stream(&remote_path).await.unwrap();

            let mut streamed_len = 0;
            while let Some(chunk) = response_data_stream.bytes().next().await {
                let chunk = chunk.unwrap();
                let end = streamed_len + chunk.len();
                assert!(end <= content.len(), "stream returned too many bytes");
                assert!(
                    content[streamed_len..end] == chunk[..],
                    "streamed bytes differ from uploaded bytes at offset {streamed_len}"
                );
                streamed_len = end;
            }
            assert_eq!(streamed_len, content.len());
        }

        let response_data = bucket.delete_object(&remote_path).await.unwrap();
        assert_eq!(response_data.status_code(), 204);
        std::fs::remove_file(local_path).unwrap_or(());
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
    async fn streaming_aws_put_head_get_delete_object() {
        streaming_test_put_get_delete_small_object(test_aws_bucket()).await;
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
    async fn streaming_gc_put_head_get_delete_object() {
        streaming_test_put_get_delete_small_object(test_gc_bucket()).await;
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
    async fn streaming_r2_put_head_get_delete_object() {
        streaming_test_put_get_delete_small_object(test_r2_bucket()).await;
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
    async fn streaming_minio_put_head_get_delete_object() {
        streaming_test_put_get_delete_small_object(test_minio_bucket()).await;
    }

    #[maybe_async::maybe_async]
    async fn streaming_test_put_get_delete_small_object(bucket: Box<Bucket>) {
        init();
        let remote_path = test_object_key("+stream_test_small");
        let content: Vec<u8> = object(1000);
        #[cfg(feature = "with-tokio")]
        let mut reader = std::io::Cursor::new(&content);
        #[cfg(feature = "with-async-std")]
        let mut reader = async_std::io::Cursor::new(&content);
        #[cfg(feature = "sync")]
        let mut reader = std::io::Cursor::new(&content);

        #[cfg(not(feature = "sync"))]
        let response = bucket
            .put_object_stream_builder(&remote_path)
            .with_content_type("application/x-rust-s3-stream-test")
            .with_headers(streaming_builder_test_headers())
            .execute_stream(&mut reader)
            .await
            .unwrap();
        #[cfg(feature = "sync")]
        let response = bucket
            .put_object_stream(&mut reader, &remote_path)
            .await
            .unwrap();
        #[cfg(not(feature = "sync"))]
        assert_eq!(response.status_code(), 200);
        #[cfg(feature = "sync")]
        assert_eq!(response, 200);
        #[cfg(not(feature = "sync"))]
        {
            let (head, code) = bucket.head_object(&remote_path).await.unwrap();
            assert_eq!(code, 200);
            assert_eq!(
                head.content_type.as_deref(),
                Some("application/x-rust-s3-stream-test")
            );
            assert_eq!(head.cache_control.as_deref(), Some("public, max-age=120"));
            assert_eq!(
                head.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("stream-marker"))
                    .map(String::as_str),
                Some("multipart-builder")
            );
        }
        let mut writer = Vec::new();
        let code = bucket
            .get_object_to_writer(&remote_path, &mut writer)
            .await
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(content, writer);

        let response_data = bucket.delete_object(&remote_path).await.unwrap();
        assert_eq!(response_data.status_code(), 204);
    }

    #[cfg(all(
        not(feature = "sync"),
        any(feature = "with-tokio", feature = "with-async-std")
    ))]
    fn streaming_builder_test_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=120"),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-stream-marker"),
            HeaderValue::from_static("multipart-builder"),
        );
        headers
    }

    #[cfg(feature = "blocking")]
    fn put_head_get_list_delete_object_blocking(bucket: Bucket) {
        let s3_key = test_object_key("test_blocking.file");
        let s3_key_2 = test_object_key("test_blocking.file2");
        let s3_key_3 = test_object_key("test_blocking.file3");
        let s3_path = format!("/{s3_key}");
        let s3_path_2 = format!("/{s3_key_2}");
        let s3_path_3 = format!("/{s3_key_3}");
        let list_prefix = test_object_key("test_blocking.");
        let test: Vec<u8> = object(3072);

        // Test PutObject
        let response_data = bucket.put_object_blocking(&s3_path, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);

        // Test GetObject
        let response_data = bucket.get_object_blocking(&s3_path).unwrap();
        assert_eq!(response_data.status_code(), 200);
        assert_eq!(test, response_data.as_slice());

        // Test GetObject with a range
        let response_data = bucket
            .get_object_range_blocking(&s3_path, 100, Some(1000))
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(test[100..1001].to_vec(), response_data.as_slice());

        // Test single-byte range read (start == end)
        let response_data = bucket
            .get_object_range_blocking(&s3_path, 100, Some(100))
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(vec![test[100]], response_data.as_slice());

        // Test HeadObject
        let (head_object_result, code) = bucket.head_object_blocking(&s3_path).unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);

        // Put some additional objects, so that we can test ListObjects
        let response_data = bucket.put_object_blocking(&s3_path_2, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);
        let response_data = bucket.put_object_blocking(&s3_path_3, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);

        // Test ListObjects, with continuation
        let (result, code) = bucket
            .list_page_blocking(
                list_prefix.clone(),
                Some("/".to_string()),
                None,
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(result.contents.len(), 2);
        assert!(result.is_truncated);
        let mut listed_keys: Vec<String> =
            result.contents.into_iter().map(|item| item.key).collect();

        let cont_token = result
            .next_continuation_token
            .expect("truncated listing should include a continuation token");

        let (result, code) = bucket
            .list_page_blocking(
                list_prefix,
                Some("/".to_string()),
                Some(cont_token),
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(result.contents.len(), 1);
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
        listed_keys.extend(result.contents.into_iter().map(|item| item.key));
        listed_keys.sort();
        let mut expected_keys = vec![s3_key, s3_key_2, s3_key_3];
        expected_keys.sort();
        assert_eq!(listed_keys, expected_keys);

        // cleanup (and test Delete)
        let response_data = bucket.delete_object_blocking(&s3_path).unwrap();
        assert_eq!(response_data.status_code(), 204);
        let response_data = bucket.delete_object_blocking(&s3_path_2).unwrap();
        assert_eq!(response_data.status_code(), 204);
        let response_data = bucket.delete_object_blocking(&s3_path_3).unwrap();
        assert_eq!(response_data.status_code(), 204);
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn aws_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_aws_bucket())
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn gc_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_gc_bucket())
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn wasabi_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_wasabi_bucket())
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn minio_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_minio_bucket())
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn r2_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_r2_bucket())
    }

    #[ignore]
    #[cfg(all(
        any(feature = "with-tokio", feature = "with-async-std"),
        feature = "blocking"
    ))]
    #[test]
    fn digital_ocean_put_head_get_delete_object_blocking() {
        put_head_get_list_delete_object_blocking(*test_digital_ocean_bucket())
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
    async fn aws_put_head_get_delete_object() {
        put_head_get_delete_object(*test_aws_bucket(), true).await;
        put_head_delete_object_with_headers(*test_aws_bucket()).await;
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
    async fn gc_test_put_head_get_delete_object() {
        put_head_get_delete_object(*test_gc_bucket(), true).await;
        put_head_delete_object_with_headers(*test_gc_bucket()).await;
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
        put_head_get_delete_object(*test_wasabi_bucket(), true).await;
        put_head_delete_object_with_headers(*test_wasabi_bucket()).await;
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
    async fn minio_test_put_head_get_delete_object() {
        let bucket = *test_minio_bucket();
        put_head_get_delete_object(bucket.clone(), true).await;
        put_head_delete_object_with_headers(bucket.clone()).await;

        let prefix = test_object_key(&format!("+copy_roundtrip_{}", uuid::Uuid::new_v4()));
        let source = format!("{prefix}_source");
        let destination = format!("{prefix}_destination");
        let content = b"MinIO copy object content verification";
        let copy_result = match bucket.put_object(&source, content).await {
            Err(error) => Err(error),
            Ok(_) => match bucket.copy_object_internal(&source, &destination).await {
                Err(error) => Err(error),
                Ok(status) => bucket
                    .get_object(&destination)
                    .await
                    .map(|copied| (status, copied.to_vec())),
            },
        };
        let source_cleanup = bucket.delete_object(&source).await;
        let destination_cleanup = bucket.delete_object(&destination).await;

        assert_eq!(source_cleanup.unwrap().status_code(), 204);
        assert_eq!(destination_cleanup.unwrap().status_code(), 204);
        let (status, copied) = copy_result.unwrap();
        assert_eq!(status, 200);
        assert_eq!(copied.as_slice(), content);
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
    async fn minio_multipart_upload_pagination_uses_key_and_upload_id_markers() {
        let bucket = *test_minio_bucket();
        let key = test_object_key(&format!("multipart-pagination/{}", uuid::Uuid::new_v4()));
        let mut created_upload_ids = Vec::new();
        let listing_result =
            minio_list_multipart_pages(&bucket, &key, &mut created_upload_ids).await;

        let mut cleanup_errors = Vec::new();
        for upload_id in &created_upload_ids {
            if let Err(error) = bucket.abort_upload(&key, upload_id).await {
                cleanup_errors.push(error.to_string());
            }
        }

        let listed_ids = listing_result.unwrap_or_else(|primary| {
            panic!(
                "MinIO multipart pagination failed: {primary}; cleanup errors: {cleanup_errors:?}"
            )
        });
        assert!(
            cleanup_errors.is_empty(),
            "failed to abort test multipart uploads: {cleanup_errors:?}"
        );
        assert_eq!(listed_ids.len(), 2);
    }

    #[maybe_async::maybe_async]
    async fn minio_list_multipart_pages(
        bucket: &Bucket,
        key: &str,
        created_upload_ids: &mut Vec<String>,
    ) -> Result<Vec<String>, String> {
        for _ in 0..2 {
            let upload = bucket
                .initiate_multipart_upload(key, "application/octet-stream")
                .await
                .map_err(|error| error.to_string())?;
            created_upload_ids.push(upload.upload_id);
        }

        let (first_page, first_status) = bucket
            .list_multiparts_uploads_page(Some(key), None, None, None, Some(1))
            .await
            .map_err(|error| error.to_string())?;
        if first_status != 200 || !first_page.is_truncated || first_page.uploads.len() != 1 {
            return Err("first MinIO page did not contain one truncated upload".to_owned());
        }
        let next_key_marker = first_page
            .next_marker
            .clone()
            .ok_or_else(|| "first MinIO page omitted NextKeyMarker".to_owned())?;
        let next_upload_id_marker = first_page
            .next_upload_id_marker
            .clone()
            .ok_or_else(|| "first MinIO page omitted NextUploadIdMarker".to_owned())?;

        let (second_page, second_status) = bucket
            .list_multiparts_uploads_page(
                Some(key),
                None,
                Some(next_key_marker),
                Some(next_upload_id_marker),
                Some(1),
            )
            .await
            .map_err(|error| error.to_string())?;
        if second_status != 200 || second_page.is_truncated || second_page.uploads.len() != 1 {
            return Err("second MinIO page did not contain one upload".to_owned());
        }

        let mut listed_ids = first_page
            .uploads
            .iter()
            .chain(second_page.uploads.iter())
            .map(|upload| upload.id.clone())
            .collect::<Vec<_>>();
        listed_ids.sort();
        let mut expected_ids = created_upload_ids.clone();
        expected_ids.sort();
        if listed_ids != expected_ids {
            return Err(
                "MinIO pagination did not return each created upload exactly once".to_owned(),
            );
        }
        Ok(listed_ids)
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
    async fn digital_ocean_test_put_head_get_delete_object() {
        let bucket = *test_digital_ocean_bucket();
        put_head_get_delete_object(bucket.clone(), true).await;
        put_head_delete_object_with_headers(bucket).await;
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
    async fn r2_test_put_head_get_delete_object() {
        put_head_get_delete_object(*test_r2_bucket(), false).await;
        put_head_delete_object_with_headers(*test_r2_bucket()).await;
    }

    #[maybe_async::maybe_async]
    async fn put_delete_objects(bucket: Bucket) {
        use crate::serde_types::ObjectIdentifier;

        let paths = [
            test_object_path("+bulk_delete_1.file"),
            test_object_path("+bulk_delete_2.file"),
            test_object_path("+bulk_delete_3.file"),
        ];
        let test: Vec<u8> = object(128);

        // Put test objects
        for path in &paths {
            let response_data = bucket.put_object(path, &test).await.unwrap();
            assert_eq!(response_data.status_code(), 200);
        }

        // Bulk delete them
        let objects: Vec<ObjectIdentifier> = paths
            .iter()
            .map(|path| ObjectIdentifier::new(path.as_str()))
            .collect();
        let result = bucket.delete_objects(objects).await.unwrap();

        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify they are gone
        for path in &paths {
            let exists = bucket.object_exists(path).await.unwrap();
            assert!(!exists);
        }
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
    async fn aws_test_delete_objects() {
        put_delete_objects(*test_aws_bucket()).await;
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
    async fn minio_test_delete_objects() {
        put_delete_objects(*test_minio_bucket()).await;
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_presign_put() {
        let s3_path = "/test/test.file";
        let bucket = test_presign_bucket();

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(
            HeaderName::from_static("custom_header"),
            "custom_value".parse().unwrap(),
        );

        let url = bucket
            .presign_put(s3_path, 86400, Some(custom_headers), None)
            .await
            .unwrap();

        assert!(url.contains("custom_header%3Bhost"));
        assert!(url.contains("/test/test.file"))
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_presign_post() {
        use std::borrow::Cow;

        let bucket = test_presign_bucket();

        // Policy from sample
        let policy = PostPolicy::new(86400)
            .condition(
                PostPolicyField::Key,
                PostPolicyValue::StartsWith(Cow::from("user/user1/")),
            )
            .unwrap();

        let data = bucket.presign_post(policy).await.unwrap();

        assert_eq!(data.url, "http://localhost:9000/rust-s3");
        assert_eq!(data.fields.len(), 6);
        assert_eq!(data.dynamic_fields.len(), 1);
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_presign_get() {
        let s3_path = "/test/test.file";
        let bucket = test_presign_bucket();

        let url = bucket.presign_get(s3_path, 86400, None).await.unwrap();
        assert!(url.contains("/test/test.file?"))
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_presign_delete() {
        let s3_path = "/test/test.file";
        let bucket = test_presign_bucket();

        let url = bucket.presign_delete(s3_path, 86400).await.unwrap();
        assert!(url.contains("/test/test.file?"))
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_presign_url_standard_ports() {
        // Test that presigned URLs preserve standard ports in the host header
        // This is crucial for signature validation

        // Test with HTTP standard port 80
        let region_http_80 = Region::Custom {
            region: "eu-central-1".to_owned(),
            endpoint: "http://minio:80".to_owned(),
        };
        let credentials = Credentials::new(
            Some("test_access_key"),
            Some("test_secret_key"),
            None,
            None,
            None,
        )
        .unwrap();
        let bucket_http_80 = Bucket::new("test-bucket", region_http_80, credentials.clone())
            .unwrap()
            .with_path_style();

        let presigned_url_80 = bucket_http_80
            .presign_get("/test.file", 3600, None)
            .await
            .unwrap();
        println!("Presigned URL with port 80: {}", presigned_url_80);

        // Port 80 MUST be preserved in the URL for signature validation
        assert!(
            presigned_url_80.starts_with("http://minio:80/"),
            "URL must preserve port 80, got: {}",
            presigned_url_80
        );

        // Test with HTTPS standard port 443
        let region_https_443 = Region::Custom {
            region: "eu-central-1".to_owned(),
            endpoint: "https://minio:443".to_owned(),
        };
        let bucket_https_443 = Bucket::new("test-bucket", region_https_443, credentials.clone())
            .unwrap()
            .with_path_style();

        let presigned_url_443 = bucket_https_443
            .presign_get("/test.file", 3600, None)
            .await
            .unwrap();
        println!("Presigned URL with port 443: {}", presigned_url_443);

        // Port 443 MUST be preserved in the URL for signature validation
        assert!(
            presigned_url_443.starts_with("https://minio:443/"),
            "URL must preserve port 443, got: {}",
            presigned_url_443
        );

        // Test with non-standard port (should always include port)
        let region_http_9000 = Region::Custom {
            region: "eu-central-1".to_owned(),
            endpoint: "http://minio:9000".to_owned(),
        };
        let bucket_http_9000 = Bucket::new("test-bucket", region_http_9000, credentials)
            .unwrap()
            .with_path_style();

        let presigned_url_9000 = bucket_http_9000
            .presign_get("/test.file", 3600, None)
            .await
            .unwrap();
        assert!(
            presigned_url_9000.contains("minio:9000"),
            "Non-standard port should be preserved in URL"
        );
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

    #[test]
    fn test_tag_has_key_and_value_functions() {
        let key = "key".to_owned();
        let value = "value".to_owned();
        let tag = Tag { key, value };
        assert_eq!["key", tag.key()];
        assert_eq!["value", tag.value()];
    }

    #[test]
    #[ignore]
    fn test_builder_composition() {
        use std::time::Duration;

        let bucket = Bucket::new(
            "test-bucket",
            "eu-central-1".parse().unwrap(),
            test_aws_credentials(),
        )
        .unwrap()
        .with_request_timeout(Duration::from_secs(10))
        .unwrap();

        assert_eq!(bucket.request_timeout(), Some(Duration::from_secs(10)));
    }

    #[cfg(all(
        feature = "with-tokio",
        any(feature = "tokio-native-tls", feature = "tokio-rustls-tls")
    ))]
    #[test]
    fn with_request_timeout_preserves_tokio_client_options() {
        let bucket = Bucket::new(
            "test-bucket",
            "us-east-1".parse().unwrap(),
            Credentials::anonymous().unwrap(),
        )
        .unwrap()
        .set_dangerous_config(true, true)
        .unwrap()
        .set_proxy(reqwest::Proxy::all("http://127.0.0.1:1234").unwrap())
        .unwrap();

        let updated = bucket
            .with_request_timeout(Duration::from_millis(125))
            .unwrap();

        assert_eq!(updated.request_timeout(), Some(Duration::from_millis(125)));
        assert!(updated.client_options.proxy.is_some());
        assert!(updated.client_options.accept_invalid_certs);
        assert!(updated.client_options.accept_invalid_hostnames);
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
    async fn test_bucket_cors() {
        let bucket = test_aws_bucket();
        let rule = CorsRule::new(
            None,
            vec!["GET".to_string()],
            vec!["*".to_string()],
            None,
            None,
            None,
        );
        let expected_bucket_owner = "904662384344";
        let cors_config = CorsConfiguration::new(vec![rule]);
        let response = bucket
            .put_bucket_cors(expected_bucket_owner, &cors_config)
            .await
            .unwrap();
        assert_eq!(response.status_code(), 200);

        let cors_response = bucket.get_bucket_cors(expected_bucket_owner).await.unwrap();
        assert_eq!(cors_response, cors_config);

        let response = bucket
            .delete_bucket_cors(expected_bucket_owner)
            .await
            .unwrap();
        assert_eq!(response.status_code(), 204);
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
    async fn test_bucket_lifecycle() {
        let bucket = test_aws_bucket();

        // Create a simple lifecycle rule that expires objects with prefix "test/" after 1 day
        let rule = LifecycleRule::builder("Enabled")
            .id("test-rule")
            .filter(LifecycleFilter {
                prefix: Some("test/".to_string()),
                ..Default::default()
            })
            .expiration(Expiration {
                days: Some(1),
                ..Default::default()
            })
            .build();

        let lifecycle_config = BucketLifecycleConfiguration::new(vec![rule]);

        // Test put_bucket_lifecycle
        let response = bucket
            .put_bucket_lifecycle(lifecycle_config.clone())
            .await
            .unwrap();
        assert_eq!(response.status_code(), 200);

        // Test get_bucket_lifecycle
        let retrieved_config = bucket.get_bucket_lifecycle().await.unwrap();
        assert_eq!(retrieved_config.rules.len(), 1);
        assert_eq!(retrieved_config.rules[0].id, Some("test-rule".to_string()));
        assert_eq!(retrieved_config.rules[0].status, "Enabled");

        // Test delete_bucket_lifecycle
        let response = bucket.delete_bucket_lifecycle().await.unwrap();
        assert_eq!(response.status_code(), 204);
    }

    #[ignore]
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_bucket_exists_with_dangerous_config() {
        init();

        // This test verifies that Bucket::exists() honors the dangerous SSL config
        // which allows connections with invalid SSL certificates

        // Create a bucket with dangerous config enabled
        // Note: This test requires a test environment with self-signed or invalid certs
        // For CI, we'll test with a regular bucket but verify the config is preserved

        let credentials = test_aws_credentials();
        let region = "eu-central-1".parse().unwrap();
        let bucket_name = "rust-s3-test";

        // Create bucket with dangerous config
        let bucket = Bucket::new(bucket_name, region, credentials)
            .unwrap()
            .with_path_style();

        // Set dangerous config (allow invalid certs, allow invalid hostnames)
        let bucket = bucket.set_dangerous_config(true, true).unwrap();

        // Test that exists() works with the dangerous config
        // This should not panic or fail due to SSL certificate issues
        let exists_result = bucket.exists().await;

        // The bucket should exist (assuming test bucket is set up)
        assert!(
            exists_result.is_ok(),
            "Bucket::exists() failed with dangerous config"
        );
        let exists = exists_result.unwrap();
        assert!(exists, "Test bucket should exist");

        // Verify that the dangerous config is preserved in the cloned bucket
        // by checking if we can perform other operations
        let list_result = bucket.list("".to_string(), Some("/".to_string())).await;
        assert!(
            list_result.is_ok(),
            "List operation should work with dangerous config"
        );
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
    async fn test_bucket_exists_without_dangerous_config() {
        init();

        // This test verifies normal behavior without dangerous config
        let credentials = test_aws_credentials();
        let region = "eu-central-1".parse().unwrap();
        let bucket_name = "rust-s3-test";

        // Create bucket without dangerous config
        let bucket = Bucket::new(bucket_name, region, credentials)
            .unwrap()
            .with_path_style();

        // Test that exists() works normally
        let exists_result = bucket.exists().await;
        assert!(
            exists_result.is_ok(),
            "Bucket::exists() should work without dangerous config"
        );
        let exists = exists_result.unwrap();
        assert!(exists, "Test bucket should exist");
    }

    #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn credentials_refresh_is_singleflight_and_does_not_hold_read_lock() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Mutex, mpsc};
        use std::time::Duration;

        let mut initial =
            Credentials::new(Some("old-access"), Some("old-secret"), None, None, None).unwrap();
        initial.expiration = Some(time::OffsetDateTime::from_unix_timestamp(0).unwrap().into());
        let bucket = Bucket::new("test-bucket", "us-east-1".parse().unwrap(), initial).unwrap();
        let cloned = (*bucket).clone();

        let refresh_count = Arc::new(AtomicUsize::new(0));
        let (started_sender, started_receiver) = async_std::channel::bounded::<()>(1);
        let (release_sender, release_receiver) = mpsc::channel();
        let release_receiver = Arc::new(Mutex::new(release_receiver));

        let first_count = refresh_count.clone();
        let first_started = started_sender.clone();
        let first_release = release_receiver.clone();
        let first_refresh = bucket.credentials_refresh_with(move |mut credentials| {
            first_count.fetch_add(1, Ordering::SeqCst);
            let _ = first_started.try_send(());
            first_release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("test should release the blocked refresh");
            credentials.access_key = Some("refreshed-access".into());
            credentials.expiration =
                Some((time::OffsetDateTime::now_utc() + time::Duration::minutes(5)).into());
            Ok(credentials)
        });

        let second_count = refresh_count.clone();
        let second_started = started_sender;
        let second_release = release_receiver;
        let second_refresh = cloned.credentials_refresh_with(move |mut credentials| {
            second_count.fetch_add(1, Ordering::SeqCst);
            let _ = second_started.try_send(());
            second_release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("test should release the blocked refresh");
            credentials.access_key = Some("refreshed-access".into());
            credentials.expiration =
                Some((time::OffsetDateTime::now_utc() + time::Duration::minutes(5)).into());
            Ok(credentials)
        });

        let read_bucket = (*bucket).clone();
        let read_during_refresh = async move {
            started_receiver.recv().await.unwrap();
            let credentials =
                async_std::future::timeout(Duration::from_secs(2), read_bucket.credentials())
                    .await
                    .expect("credential reads should not wait for the blocking refresh")
                    .unwrap();
            assert_eq!(credentials.access_key.as_deref(), Some("old-access"));
            release_sender.send(()).unwrap();
            release_sender.send(()).unwrap();
        };

        let refreshes = futures_util::future::join(first_refresh, second_refresh);
        let (refresh_results, ()) = async_std::future::timeout(
            Duration::from_secs(8),
            futures_util::future::join(refreshes, read_during_refresh),
        )
        .await
        .expect("refresh coordination should finish within the test deadline");
        refresh_results.0.unwrap();
        refresh_results.1.unwrap();

        assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            bucket.credentials().await.unwrap().access_key.as_deref(),
            Some("refreshed-access")
        );
    }
}
