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
use std::collections::HashMap;
use std::time::Duration;

use crate::bucket_ops::{BucketConfiguration, CreateBucketResponse};
use crate::command::{Command, Multipart};
use crate::creds::Credentials;
use crate::region::Region;
#[cfg(feature = "with-tokio")]
use crate::request::tokio_backend::client;
#[cfg(feature = "with-tokio")]
use crate::request::tokio_backend::ClientOptions;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use crate::request::ResponseDataStream;
use crate::request::{Request as _, ResponseData};
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "with-tokio")]
use tokio::sync::RwLock;

#[cfg(feature = "with-async-std")]
use async_std::sync::RwLock;

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

use crate::error::S3Error;
use crate::post_policy::PresignedPost;
use crate::serde_types::{
    BucketLifecycleConfiguration, BucketLocationResult, CompleteMultipartUploadData,
    CorsConfiguration, GetObjectAttributesOutput, HeadObjectResult,
    InitiateMultipartUploadResponse, ListBucketResult, ListMultipartUploadsResult, Part,
};
#[allow(unused_imports)]
use crate::utils::{error_from_response_data, PutStreamResponse};
use crate::PostPolicy;
use http::header::HeaderName;
use http::HeaderMap;

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
    #[maybe_async::async_impl]
    /// Credential refreshing is done automatically, but can be manually triggered.
    pub async fn credentials_refresh(&self) -> Result<(), S3Error> {
        Ok(self.credentials.write().await.refresh()?)
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
    /// let url = bucket.presign_get("/test.file", 86400, Some(custom_queries)).await.unwrap();
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
    /// let presigned_post = bucket.presign_post(post_policy).await.unwrap();
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
    /// let url = bucket.presign_put("/test.file", 86400, Some(custom_headers), None).await.unwrap();
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
    /// let url = bucket.presign_delete("/test.file", 86400).await.unwrap();
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
        config.set_region(region.clone());
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
    /// let response = Bucket::list_buckets(region, credentials).await?;
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
        let request = RequestImpl::new(&dummy_bucket, "", Command::ListBuckets).await?;
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
        let credentials = self.credentials().await?;

        let response = Self::list_buckets(self.region.clone(), credentials).await?;

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
        config.set_region(region.clone());
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
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: Some(request_timeout),
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
        }))
    }

    #[cfg(feature = "with-tokio")]
    pub fn with_request_timeout(&self, request_timeout: Duration) -> Result<Box<Bucket>, S3Error> {
        let options = ClientOptions {
            request_timeout: Some(request_timeout),
            ..Default::default()
        };

        Ok(Box::new(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: Some(request_timeout),
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            #[cfg(feature = "with-tokio")]
            http_client: client(&options)?,
            #[cfg(feature = "with-tokio")]
            client_options: options,
        }))
    }

    pub fn with_listobjects_v1(&self) -> Bucket {
        Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
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
    ///     .set_dangereous_config(true, true)?;
    /// # Ok(())
    /// # }
    ///
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub fn set_dangereous_config(
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
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client: client(&options)?,
            client_options: options,
        })
    }

    #[cfg(feature = "with-tokio")]
    pub fn set_proxy(&self, proxy: reqwest::Proxy) -> Result<Bucket, S3Error> {
        let mut options = self.client_options.clone();
        options.proxy = Some(proxy);

        Ok(Bucket {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
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
        let response_data = match request.response_data(false).await {
            Ok(response_data) => response_data,
            Err(S3Error::HttpFailWithBody(status_code, error)) => {
                if status_code == 404 {
                    return Ok(false);
                }
                return Err(S3Error::HttpFailWithBody(status_code, error));
            }
            Err(e) => return Err(e),
        };
        Ok(response_data.status_code() != 404)
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
        let request = RequestImpl::new(self, "", Command::DeleteBucket).await?;
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
            assert!(start < end);
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
            assert!(start < end);
        }

        let command = Command::GetObjectRange { start, end };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data_to_writer(writer).await
    }

    #[maybe_async::sync_impl]
    pub async fn get_object_range_to_writer<T: std::io::Write + Send + ?Sized, S: AsRef<str>>(
        &self,
        path: S,
        start: u64,
        end: Option<u64>,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        if let Some(end) = end {
            assert!(start < end);
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
    /// // tokio open file
    /// let mut async_output_file = tokio::fs::File::create("async_output_file").await.expect("Unable to create file");
    /// file.write_all(&test)?;
    ///
    /// // Generic over std::io::Read
    /// #[cfg(feature = "with-tokio")]
    /// let status_code = bucket.put_object_stream(&mut async_output_file, "/path").await?;
    ///
    ///
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_output_file = async_std::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// // Generic over std::io::Read
    /// let status_code = bucket.put_object_stream(&mut path, "/path")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket.put_object_stream_blocking(&mut path, "/path")?;
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
    /// let mut async_output_file = tokio::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// #[cfg(feature = "with-async-std")]
    /// let mut async_output_file = async_std::fs::File::create("async_output_file").await.expect("Unable to create file");
    ///
    /// // Async variant with `tokio` or `async-std` features
    /// // Generic over std::io::Read
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type(&mut async_output_file, "/path", "application/octet-stream")
    ///     .await?;
    ///
    /// // `sync` feature will produce an identical method
    /// #[cfg(feature = "sync")]
    /// // Generic over std::io::Read
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type(&mut path, "/path", "application/octet-stream")?;
    ///
    /// // Blocking variant, generated with `blocking` feature in combination
    /// // with `tokio` or `async-std` features.
    /// #[cfg(feature = "blocking")]
    /// let status_code = bucket
    ///     .put_object_stream_with_content_type_blocking(&mut path, "/path", "application/octet-stream")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
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

    #[maybe_async::async_impl]
    async fn make_multipart_request(
        &self,
        path: &str,
        chunk: Vec<u8>,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<ResponseData, S3Error> {
        let command = Command::PutObject {
            content: &chunk,
            multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
            content_type,
        };
        let request = RequestImpl::new(self, path, command).await?;
        request.response_data(true).await
    }

    #[maybe_async::async_impl]
    async fn _put_object_stream_with_content_type<R: AsyncRead + Unpin + ?Sized>(
        &self,
        reader: &mut R,
        s3_path: &str,
        content_type: &str,
    ) -> Result<PutStreamResponse, S3Error> {
        // If the file is smaller CHUNK_SIZE, just do a regular upload.
        // Otherwise perform a multi-part upload.
        let first_chunk = crate::utils::read_chunk_async(reader).await?;
        // println!("First chunk size: {}", first_chunk.len());
        if first_chunk.len() < CHUNK_SIZE {
            let total_size = first_chunk.len();
            let response_data = self
                .put_object_with_content_type(s3_path, first_chunk.as_slice(), content_type)
                .await?;
            if response_data.status_code() >= 300 {
                return Err(error_from_response_data(response_data)?);
            }
            return Ok(PutStreamResponse::new(
                response_data.status_code(),
                total_size,
            ));
        }

        let msg = self
            .initiate_multipart_upload(s3_path, content_type)
            .await?;
        let path = msg.key;
        let upload_id = &msg.upload_id;

        let mut part_number: u32 = 0;
        let mut etags = Vec::new();

        // Collect request handles
        let mut handles = vec![];
        let mut total_size = 0;
        loop {
            let chunk = if part_number == 0 {
                first_chunk.clone()
            } else {
                crate::utils::read_chunk_async(reader).await?
            };
            total_size += chunk.len();

            let done = chunk.len() < CHUNK_SIZE;

            // Start chunk upload
            part_number += 1;
            handles.push(self.make_multipart_request(
                &path,
                chunk,
                part_number,
                upload_id,
                content_type,
            ));

            if done {
                break;
            }
        }

        // Wait for all chunks to finish (or fail)
        let responses = futures::future::join_all(handles).await;

        for response in responses {
            let response_data = response?;
            if !(200..300).contains(&response_data.status_code()) {
                // if chunk upload failed - abort the upload
                match self.abort_upload(&path, upload_id).await {
                    Ok(_) => {
                        return Err(error_from_response_data(response_data)?);
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }

            let etag = response_data.as_str()?;
            etags.push(etag.to_string());
        }

        // Finish the upload
        let inner_data = etags
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, x)| Part {
                etag: x,
                part_number: i as u32 + 1,
            })
            .collect::<Vec<Part>>();
        let response_data = self
            .complete_multipart_upload(&path, &msg.upload_id, inner_data)
            .await?;

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

        let mut part_number: u32 = 0;
        let mut etags = Vec::new();
        loop {
            let chunk = crate::utils::read_chunk(reader)?;

            if chunk.len() < CHUNK_SIZE {
                if part_number == 0 {
                    // Files is not big enough for multipart upload, going with regular put_object
                    self.abort_upload(&path, upload_id)?;

                    return Ok(self.put_object(s3_path, chunk.as_slice())?.status_code());
                } else {
                    part_number += 1;
                    let part = self.put_multipart_chunk(
                        &chunk,
                        &path,
                        part_number,
                        upload_id,
                        content_type,
                    )?;
                    etags.push(part.etag);
                    let inner_data = etags
                        .into_iter()
                        .enumerate()
                        .map(|(i, x)| Part {
                            etag: x,
                            part_number: i as u32 + 1,
                        })
                        .collect::<Vec<Part>>();
                    return Ok(self
                        .complete_multipart_upload(&path, upload_id, inner_data)?
                        .status_code());
                    // let response = std::str::from_utf8(data.as_slice())?;
                }
            } else {
                part_number += 1;
                let part =
                    self.put_multipart_chunk(&chunk, &path, part_number, upload_id, content_type)?;
                etags.push(part.etag.to_string());
            }
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

    /// Completes a previously initiated multipart upload, with optional final data chunks
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
        complete_request.response_data(false).await
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
        complete_request.response_data(false)
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
            multipart: None,
        };
        let request = RequestImpl::new(self, path.as_ref(), command).await?;
        request.response_data(true).await
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

    #[maybe_async::maybe_async]
    pub async fn list_multiparts_uploads_page(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<String>,
        max_uploads: Option<usize>,
    ) -> Result<(ListMultipartUploadsResult, u16), S3Error> {
        let command = Command::ListMultipartUploads {
            prefix,
            delimiter,
            key_marker,
            max_uploads,
        };
        let request = RequestImpl::new(self, "/", command).await?;
        let response_data = request.response_data(false).await?;
        let list_bucket_result = quick_xml::de::from_reader(response_data.as_slice())?;

        Ok((list_bucket_result, response_data.status_code()))
    }

    /// List the ongoing multipart uploads of an S3 bucket. This may be useful to cleanup failed
    /// uploads, together with [`crate::bucket::Bucket::abort_upload`].
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
    #[allow(clippy::assigning_clones)]
    pub async fn list_multiparts_uploads(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Vec<ListMultipartUploadsResult>, S3Error> {
        let the_bucket = self.to_owned();
        let mut results = Vec::new();
        let mut next_marker: Option<String> = None;

        loop {
            let (list_multiparts_uploads_result, _) = the_bucket
                .list_multiparts_uploads_page(prefix, delimiter, next_marker, None)
                .await?;

            let is_truncated = list_multiparts_uploads_result.is_truncated;

            next_marker = list_multiparts_uploads_result.next_marker.clone();
            results.push(list_multiparts_uploads_result);

            if !is_truncated {
                break;
            }
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

    /// Configure bucket to apply this request timeout to all HTTP
    /// requests, or no (infinity) timeout if `None`.  Defaults to
    /// 30 seconds.
    ///
    /// Only the [`attohttpc`] and the [`hyper`] backends obey this option;
    /// async code may instead await with a timeout.
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

    use crate::creds::Credentials;
    use crate::post_policy::{PostPolicyField, PostPolicyValue};
    use crate::region::Region;
    use crate::serde_types::CorsConfiguration;
    use crate::serde_types::CorsRule;
    use crate::BucketConfiguration;
    use crate::Tag;
    use crate::{Bucket, PostPolicy};
    use http::header::HeaderName;
    use http::HeaderMap;
    use std::env;

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
        let s3_path = "/+test.file";
        let non_existant_path = "/+non_existant.file";
        let test: Vec<u8> = object(3072);

        let response_data = bucket.put_object(s3_path, &test).await.unwrap();
        assert_eq!(response_data.status_code(), 200);

        // let attributes = bucket
        //     .get_object_attributes(s3_path, "904662384344", None)
        //     .await
        //     .unwrap();

        let response_data = bucket.get_object(s3_path).await.unwrap();
        assert_eq!(response_data.status_code(), 200);
        assert_eq!(test, response_data.as_slice());

        let exists = bucket.object_exists(s3_path).await.unwrap();
        assert!(exists);

        let not_exists = bucket.object_exists(non_existant_path).await.unwrap();
        assert!(!not_exists);

        let response_data = bucket
            .get_object_range(s3_path, 100, Some(1000))
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(test[100..1001].to_vec(), response_data.as_slice());
        if head {
            let (_head_object_result, code) = bucket.head_object(s3_path).await.unwrap();
            // println!("{:?}", head_object_result);
            assert_eq!(code, 200);
        }

        // println!("{:?}", head_object_result);
        let response_data = bucket.delete_object(s3_path).await.unwrap();
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
        let _target_tags = vec![
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
            .put_object("tagging_test", b"Gimme tags")
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (tags, _code) = bucket.get_object_tagging("tagging_test").await.unwrap();
        assert_eq!(tags, empty_tags);
        let response_data = bucket
            .put_object_tagging("tagging_test", &[("Tag1", "Value1"), ("Tag2", "Value2")])
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        // This could be eventually consistent now
        let (_tags, _code) = bucket.get_object_tagging("tagging_test").await.unwrap();
        // assert_eq!(tags, target_tags)
        let _response_data = bucket.delete_object("tagging_test").await.unwrap();
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
        let _target_tags = vec![
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
            .put_object("tagging_test", b"Gimme tags")
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        let (tags, _code) = bucket.get_object_tagging("tagging_test").await.unwrap();
        assert_eq!(tags, empty_tags);
        let response_data = bucket
            .put_object_tagging("tagging_test", &[("Tag1", "Value1"), ("Tag2", "Value2")])
            .await
            .unwrap();
        assert_eq!(response_data.status_code(), 200);
        // This could be eventually consistent now
        let (_tags, _code) = bucket.get_object_tagging("tagging_test").await.unwrap();
        // assert_eq!(tags, target_tags)
        let _response_data = bucket.delete_object("tagging_test").await.unwrap();
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
        async(
            all(
                not(feature = "sync"),
                not(feature = "tokio-rustls-tls"),
                feature = "with-tokio"
            ),
            tokio::test
        ),
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
        use futures::StreamExt;
        #[cfg(not(any(feature = "with-tokio", feature = "with-async-std")))]
        use std::fs::File;
        #[cfg(not(any(feature = "with-tokio", feature = "with-async-std")))]
        use std::io::Write;
        #[cfg(feature = "with-tokio")]
        use tokio::fs::File;
        #[cfg(feature = "with-tokio")]
        use tokio::io::AsyncWriteExt;

        init();
        let remote_path = "+stream_test_big";
        let local_path = "+stream_test_big";
        std::fs::remove_file(remote_path).unwrap_or(());
        let content: Vec<u8> = object(20_000_000);

        let mut file = File::create(local_path).await.unwrap();
        file.write_all(&content).await.unwrap();
        file.flush().await.unwrap();
        let mut reader = File::open(local_path).await.unwrap();

        let response = bucket
            .put_object_stream(&mut reader, remote_path)
            .await
            .unwrap();
        #[cfg(not(feature = "sync"))]
        assert_eq!(response.status_code(), 200);
        #[cfg(feature = "sync")]
        assert_eq!(response, 200);
        let mut writer = Vec::new();
        let code = bucket
            .get_object_to_writer(remote_path, &mut writer)
            .await
            .unwrap();
        assert_eq!(code, 200);
        // assert_eq!(content, writer);
        assert_eq!(content.len(), writer.len());
        assert_eq!(content.len(), 20_000_000);

        #[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
        {
            let mut response_data_stream = bucket.get_object_stream(remote_path).await.unwrap();

            let mut bytes = vec![];

            while let Some(chunk) = response_data_stream.bytes().next().await {
                bytes.push(chunk)
            }
            assert_ne!(bytes.len(), 0);
        }

        let response_data = bucket.delete_object(remote_path).await.unwrap();
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
        async(
            all(
                not(feature = "sync"),
                not(feature = "tokio-rustls-tls"),
                feature = "with-tokio"
            ),
            tokio::test
        ),
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
        let remote_path = "+stream_test_small";
        let content: Vec<u8> = object(1000);
        #[cfg(feature = "with-tokio")]
        let mut reader = std::io::Cursor::new(&content);
        #[cfg(feature = "with-async-std")]
        let mut reader = async_std::io::Cursor::new(&content);
        #[cfg(feature = "sync")]
        let mut reader = std::io::Cursor::new(&content);

        let response = bucket
            .put_object_stream(&mut reader, remote_path)
            .await
            .unwrap();
        #[cfg(not(feature = "sync"))]
        assert_eq!(response.status_code(), 200);
        #[cfg(feature = "sync")]
        assert_eq!(response, 200);
        let mut writer = Vec::new();
        let code = bucket
            .get_object_to_writer(remote_path, &mut writer)
            .await
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(content, writer);

        let response_data = bucket.delete_object(remote_path).await.unwrap();
        assert_eq!(response_data.status_code(), 204);
    }

    #[cfg(feature = "blocking")]
    fn put_head_get_list_delete_object_blocking(bucket: Bucket) {
        let s3_path = "/test_blocking.file";
        let s3_path_2 = "/test_blocking.file2";
        let s3_path_3 = "/test_blocking.file3";
        let test: Vec<u8> = object(3072);

        // Test PutObject
        let response_data = bucket.put_object_blocking(s3_path, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);

        // Test GetObject
        let response_data = bucket.get_object_blocking(s3_path).unwrap();
        assert_eq!(response_data.status_code(), 200);
        assert_eq!(test, response_data.as_slice());

        // Test GetObject with a range
        let response_data = bucket
            .get_object_range_blocking(s3_path, 100, Some(1000))
            .unwrap();
        assert_eq!(response_data.status_code(), 206);
        assert_eq!(test[100..1001].to_vec(), response_data.as_slice());

        // Test HeadObject
        let (head_object_result, code) = bucket.head_object_blocking(s3_path).unwrap();
        assert_eq!(code, 200);
        assert_eq!(
            head_object_result.content_type.unwrap(),
            "application/octet-stream".to_owned()
        );
        // println!("{:?}", head_object_result);

        // Put some additional objects, so that we can test ListObjects
        let response_data = bucket.put_object_blocking(s3_path_2, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);
        let response_data = bucket.put_object_blocking(s3_path_3, &test).unwrap();
        assert_eq!(response_data.status_code(), 200);

        // Test ListObjects, with continuation
        let (result, code) = bucket
            .list_page_blocking(
                "test_blocking.".to_string(),
                Some("/".to_string()),
                None,
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(result.contents.len(), 2);
        assert_eq!(result.contents[0].key, s3_path[1..]);
        assert_eq!(result.contents[1].key, s3_path_2[1..]);

        let cont_token = result.next_continuation_token.unwrap();

        let (result, code) = bucket
            .list_page_blocking(
                "test_blocking.".to_string(),
                Some("/".to_string()),
                Some(cont_token),
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(result.contents.len(), 1);
        assert_eq!(result.contents[0].key, s3_path_3[1..]);
        assert!(result.next_continuation_token.is_none());

        // cleanup (and test Delete)
        let response_data = bucket.delete_object_blocking(s3_path).unwrap();
        assert_eq!(code, 200);
        let response_data = bucket.delete_object_blocking(s3_path_2).unwrap();
        assert_eq!(code, 200);
        let response_data = bucket.delete_object_blocking(s3_path_3).unwrap();
        assert_eq!(code, 200);
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
        put_head_get_delete_object(*test_gc_bucket(), true).await;
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
        put_head_get_delete_object(*test_minio_bucket(), true).await;
    }

    // Keeps failing on tokio-rustls-tls
    // #[ignore]
    // #[maybe_async::test(
    //     feature = "sync",
    //     async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
    //     async(
    //         all(not(feature = "sync"), feature = "with-async-std"),
    //         async_std::test
    //     )
    // )]
    // async fn digital_ocean_test_put_head_get_delete_object() {
    //     put_head_get_delete_object(test_digital_ocean_bucket(), true).await;
    // }

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
        let bucket = test_minio_bucket();

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

        let bucket = test_minio_bucket();

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
        let bucket = test_minio_bucket();

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
        let bucket = test_minio_bucket();

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
}
