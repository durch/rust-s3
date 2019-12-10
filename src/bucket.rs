use std::collections::HashMap;
use std::mem;

use serde_xml;

use command::Command;
use credentials::Credentials;
use error::{S3Error, Result};
use futures::future::{loop_fn, Loop};
use futures::Future;
use region::Region;
use request::{Headers, Query, Request};
use serde_types::Tagging;
use serde_types::{BucketLocationResult, ListBucketResult};
use std::io::Write;

/// # Example
/// ```
/// # // Fake  credentials so we don't access user's real credentials in tests
/// # use std::env;
/// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
/// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// use s3::bucket::Bucket;
/// use s3::credentials::Credentials;
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::default();
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
    /// # // Fake  credentials so we don't access user's real credentials in tests
    /// # use std::env;
    /// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    /// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
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
        })
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (data, code) = bucket.get_object("/test.file").unwrap();
    /// println!("Code: {}\nData: {:?}", code, data);
    /// ```
    pub fn get_object(&self, path: &str) -> Result<(Vec<u8>, u16)> {
        let command = Command::GetObject;
        let request = Request::new(self, path, command);
        Ok(request.response_data()?)
    }

    /// Gets file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// extern crate futures;
    ///
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    /// use futures::Future;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// bucket.get_object_async("/test.file")
    ///     .map(|(data, code)| {
    ///         println!("Code: {}", code);
    ///         println!("{:?}", data);
    /// });
    /// ```
    pub fn get_object_async(&self, path: &str) -> impl Future<Output = Result<(Vec<u8>, u16)>> {
        let command = Command::GetObject;
        let request = Request::new(self, path, command);
        request.response_data_future()
    }

    /// Stream file from S3 path to a local file, generic over T: Write.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    /// use std::fs::File;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    /// let code = bucket.get_object_stream("/test.file", &mut output_file).unwrap();
    /// println!("Code: {}", code);
    /// ```
    pub fn get_object_stream<T: Write>(&self, path: &str, writer: &mut T) -> Result<u16> {
        let command = Command::GetObject;
        let request = Request::new(self, path, command);
        Ok(request.response_data_to_writer(writer)?)
    }

    /// Stream file from S3 path to a local file, generic over T: Write.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    ///
    /// extern crate futures;
    ///
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    /// use std::fs::File;
    /// use futures::Future;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// let mut output_file = File::create("output_file").expect("Unable to create file");
    ///
    /// bucket.get_object_stream_async("/test.file", &mut output_file)
    ///     .map(|status_code| println!("Code: {}", status_code));
    ///
    /// ```
    pub fn get_object_stream_async<'b, T: Write>(
        &self,
        path: &str,
        writer: &'b mut T,
    ) -> impl Future<Output = Result<u16>> + 'b {
        let command = Command::GetObject;
        let request = Request::new(self, path, command);
        request.response_data_to_writer_future(writer)
    }

    //// Get bucket location from S3
    ////
    /// # Example
    /// ```rust,no_run
    /// # // Fake  credentials so we don't access user's real credentials in tests
    /// # use std::env;
    /// # env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    /// # env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = "rust-s3-test";
    /// let region = "eu-central-1".parse().unwrap();
    /// let credentials = Credentials::default();
    ///
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    /// println!("{}", bucket.location().unwrap().0)
    /// ```
    pub fn location(&self) -> Result<(Region, u16)> {
        let request = Request::new(self, "?location", Command::GetBucketLocation);
        let result = request.response_data()?;
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
    /// ```rust,no_run
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.delete_object("/test.file").unwrap();
    /// assert_eq!(204, code);
    /// ```
    pub fn delete_object(&self, path: &str) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObject;
        let request = Request::new(self, path, command);
        request.response_data()
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let content = "I want to go to S3".as_bytes();
    /// let (_, code) = bucket.put_object("/test.file", content, "text/plain").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put_object(
        &self,
        path: &str,
        content: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16)> {
        let command = Command::PutObject {
            content,
            content_type,
        };
        let request = Request::new(self, path, command);
        Ok(request.response_data()?)
    }

    fn _tags_xml(&self, tags: &[(&str, &str)]) -> String {
        let mut s = String::new();
        let content = tags
            .iter()
            .map(|&(name, value)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", name, value))
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.put_object_tagging("/test.file", &[("Tag1", "Value1"), ("Tag2", "Value2")]).unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put_object_tagging(
        &self,
        path: &str,
        tags: &[(&str, &str)],
    ) -> Result<(Vec<u8>, u16)> {
        let content = self._tags_xml(&tags);
        let command = Command::PutObjectTagging {
            tags: &content.to_string(),
        };
        let request = Request::new(self, path, command);
        Ok(request.response_data()?)
    }

    /// Delete tags from an S3 object.
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (_, code) = bucket.delete_object_tagging("/test.file").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn delete_object_tagging(&self, path: &str) -> Result<(Vec<u8>, u16)> {
        let command = Command::DeleteObjectTagging;
        let request = Request::new(self, path, command);
        Ok(request.response_data()?)
    }

    /// Retrieve an S3 object list of tags.
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let (tags, code) = bucket.get_object_tagging("/test.file").unwrap();
    /// if code == 200 {
    ///     for tag in tags.expect("No tags found").tag_set {
    ///         println!("{}={}", tag.key(), tag.value());
    ///     }
    /// }
    /// ```
    pub fn get_object_tagging(&self, path: &str) -> Result<(Option<Tagging>, u16)> {
        let command = Command::GetObjectTagging {};
        let request = Request::new(self, path, command);
        let result = request.response_data()?;

        let tagging = if result.1 == 200 {
            let result_string = String::from_utf8_lossy(&result.0);
            println!("{}", result_string);
            Some(serde_xml::from_reader(result_string.as_bytes())?)
        } else {
            None
        };

        Ok((tagging, result.1))
    }

    pub fn list_page(
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
        let result = request.response_data()?;
        let result_string = String::from_utf8_lossy(&result.0);
        match serde_xml::from_reader(result_string.as_bytes()) {
            Ok(list_bucket_result) => Ok((list_bucket_result, result.1)),
            Err(_) => {
                let mut err = S3Error::from("Could not deserialize result");
                err.data = Some(result_string.to_string());
                Err(err)
            }
        }
    }

    pub fn list_page_async(
        &self,
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
    ) -> impl Future<Output = Result<(ListBucketResult, u16)>> + Send {
        let command = Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
        };
        let request = Request::new(self, "/", command);
        let result = request.response_data_future();
        result.then(|(response, status_code)| {
            match serde_xml::from_reader(response.as_slice()) {
                Ok(list_bucket_result) => Ok((list_bucket_result, status_code)),
                Err(_) => {
                    let mut err = S3Error::from("Could not deserialize result");
                    err.data = Some(String::from_utf8_lossy(response.as_slice()).to_string());
                    Err(err)
                }
            }
        })
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let results = bucket.list_all("/".to_string(), Some("/".to_string())).unwrap();
    /// for (list, code) in results {
    ///     assert_eq!(200, code);
    ///     println!("{:?}", list);
    /// }
    /// ```
    pub fn list_all(
        &self,
        prefix: String,
        delimiter: Option<String>,
    ) -> Result<Vec<(ListBucketResult, u16)>> {
        let mut results = Vec::new();
        let mut result = self.list_page(prefix.clone(), delimiter.clone(), None)?;
        loop {
            results.push(result.clone());
            match result.0.next_continuation_token {
                Some(token) => {
                    result = self.list_page(prefix.clone(), delimiter.clone(), Some(token))?
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// List the contents of an S3 bucket.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// extern crate futures;
    /// use std::str;
    /// use s3::bucket::Bucket;
    /// use s3::credentials::Credentials;
    /// use futures::future::Future;
    /// let bucket_name = &"rust-s3-test";
    /// let aws_access = &"access_key";
    /// let aws_secret = &"secret_key";
    ///
    /// let bucket_name = &"rust-s3-test";
    /// let region = "us-east-1".parse().unwrap();
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let results = bucket.list_all_async("/".to_string(), Some("/".to_string())).and_then(|list| {
    ///     println!("{:?}", list);
    ///     Ok(())
    /// });
    /// ```
    pub fn list_all_async(
        &self,
        prefix: String,
        delimiter: Option<String>,
    ) -> impl Future<Output = Result<Vec<ListBucketResult>>> + Send {
        let the_bucket = self.to_owned();
        let list_entire_bucket = loop_fn(
            (None, Vec::new()),
            move |(continuation_token, results): (Option<String>, Vec<ListBucketResult>)| {
                let mut inner_results = results;
                the_bucket
                    .list_page_async(prefix.clone(), delimiter.clone(), continuation_token)
                    .and_then(|(result, _status_code)| {
                        inner_results.push(result.clone());
                        match result.next_continuation_token {
                            Some(token) => Ok(Loop::Continue((Some(token), inner_results))),
                            None => Ok(Loop::Break((None, inner_results))),
                        }
                    })
            },
        );
        list_entire_bucket
            .and_then(|(_token, results): (Option<&str>, Vec<ListBucketResult>)| Ok(results))
    }

    /// Get a reference to the name of the S3 bucket.
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    /// Get a reference to the hostname of the S3 API endpoint.
    pub fn host(&self) -> String {
        self.region.host()
    }

    pub fn self_host(&self) -> String {
        format!("{}.s3.amazonaws.com", self.name)
    }

    pub fn scheme(&self) -> String {
        self.region.scheme()
    }

    /// Get the region this object will connect to.
    pub fn region(&self) -> Region {
        self.region.clone()
    }

    /// Get a reference to the AWS access key.
    pub fn access_key(&self) -> String {
        self.credentials.access_key.clone()
    }

    /// Get a reference to the AWS secret key.
    pub fn secret_key(&self) -> String {
        self.credentials.secret_key.clone()
    }

    /// Get a reference to the AWS token.
    pub fn token(&self) -> Option<&str> {
        self.credentials
            .token
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
