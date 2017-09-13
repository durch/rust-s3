use std::collections::HashMap;
use std::mem;

use serde_xml;

use credentials::Credentials;
use command::Command;
use region::Region;
use request::{Request, Headers, Query};
use serde_types::ListBucketResult;
use error::S3Result;

/// # Example
/// ```
/// use s3::bucket::Bucket;
/// use s3::credentials::Credentials;
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::new("access_key", "secret_key", None);
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
}

impl Bucket {
    /// Instantiate a new `Bucket`.
    ///
    /// # Example
    /// ```
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::new("access_key", "secret_key", None);
    ///
    /// let bucket = Bucket::new(bucket_name, region, credentials);
    /// ```
    pub fn new(name: &str, region: Region, credentials: Credentials) -> Bucket {
        Bucket {
            name: name.into(),
            region: region,
            credentials: credentials,
            extra_headers: HashMap::new(),
            extra_query: HashMap::new(),
        }
    }

    /// Gets file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::new("access_key", "secret_key", None);
    /// let bucket = Bucket::new(bucket_name, region, credentials);
    ///
    /// let (data, code) = bucket.get("/test.file").unwrap();
    /// println!("Code: {}\nData: {:?}", code, data);
    /// ```
    pub fn get(&self, path: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::Get;
        let request = Request::new(self, path, command);
        request.execute()
    }

    /// Delete file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::new("access_key", "secret_key", None);
    /// let bucket = Bucket::new(bucket_name, region, credentials);
    ///
    /// let (_, code) = bucket.delete("/test.file").unwrap();
    /// assert_eq!(204, code);
    /// ```
    pub fn delete(&self, path: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::Delete;
        let request = Request::new(self, path, command);
        request.execute()
    }

    /// Put into an S3 bucket.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::new("access_key", "secret_key", None);
    /// let bucket = Bucket::new(bucket_name, region, credentials);
    ///
    /// let content = "I want to go to S3".as_bytes();
    /// let (_, code) = bucket.put("/test.file", content, "text/plain").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put(&self, path: &str, data: &[u8], content_type: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::Put {
            content: data,
            content_type: content_type,
        };
        let request = Request::new(self, path, command);
        request.execute()
    }

    /// List the contents of an S3 bucket.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::str;
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::new("access_key", "secret_key", None);
    /// let bucket = Bucket::new(bucket_name, region, credentials);
    ///
    /// let (list, code) = bucket.list("/", Some("/")).unwrap();
    /// assert_eq!(200, code);
    /// println!("{:?}", list);
    /// ```
    pub fn list(&self, prefix: &str, delimiter: Option<&str>) -> S3Result<(ListBucketResult, u32)> {
        let command = Command::List {
            prefix: prefix,
            delimiter: delimiter,
        };
        let request = Request::new(self, "/", command);
        let result = request.execute()?;
        let result_string = String::from_utf8_lossy(&result.0);
        let deserialized: ListBucketResult = serde_xml::deserialize(result_string.as_bytes())?;
        Ok((deserialized, result.1))
    }

    /// Get a reference to the name of the S3 bucket.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a reference to the hostname of the S3 API endpoint.
    pub fn host(&self) -> &str {
        self.region.endpoint()
    }

    /// Get the region this object will connect to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Get a reference to the AWS access key.
    pub fn access_key(&self) -> &str {
        &self.credentials.access_key
    }

    /// Get a reference to the AWS secret key.
    pub fn secret_key(&self) -> &str {
        &self.credentials.secret_key
    }

    /// Get a reference to the AWS token.
    pub fn token(&self) -> Option<&str> {
        self.credentials.token.as_ref().map(|s| s.as_str())
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
