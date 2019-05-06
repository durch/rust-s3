use std::collections::HashMap;
use std::mem;

use serde_xml;

use credentials::Credentials;
use command::Command;
use region::Region;
use request::{Request, Headers, Query};
use serde_types::{ListBucketResult, BucketLocationResult};
use error::S3Result;
use serde_types::Tagging;

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
    pub fn new(name: &str, region: Region, credentials: Credentials) -> S3Result<Bucket> {
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
    pub fn get_object(&self, path: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::GetObject;
        let request = Request::new(self, path, command);
        request.execute()
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
    pub fn location(&self) -> S3Result<(Region, u32)> {
        let request = Request::new(self, "?location", Command::GetBucketLocation);
        let result = request.execute()?;
        let result_string = String::from_utf8_lossy(&result.0);
        let region = match serde_xml::deserialize(result_string.as_bytes()) {
            Ok(r) => {
                let location_result: BucketLocationResult = r;
                location_result.region.parse()?
            }
            Err(e) => {
                if e.to_string() == "missing field `$value`" {
                    "us-east-1".parse()?
                } else {
                    bail!(e)
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
    pub fn delete_object(&self, path: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::DeleteObject;
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
    /// let credentials = Credentials::default();
    /// let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    ///
    /// let content = "I want to go to S3".as_bytes();
    /// let (_, code) = bucket.put_object("/test.file", content, "text/plain").unwrap();
    /// assert_eq!(201, code);
    /// ```
    pub fn put_object(&self, path: &str, content: &[u8], content_type: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::PutObject {
            content,
            content_type,
        };
        let request = Request::new(self, path, command);
        request.execute()
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
    pub fn put_object_tagging(&self, path: &str, tags: &[(&str, &str)]) -> S3Result<(Vec<u8>, u32)> {
        let content = self._tags_xml(&tags);
        let command = Command::PutObjectTagging {
            tags: &content.to_string()
        };
        let request = Request::new(self, path, command);
        request.execute()
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
    pub fn delete_object_tagging(&self, path: &str) -> S3Result<(Vec<u8>, u32)> {
        let command = Command::DeleteObjectTagging;
        let request = Request::new(self, path, command);
        request.execute()
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
    pub fn get_object_tagging(&self, path: &str) -> S3Result<(Option<Tagging>, u32)> {
        let command = Command::GetObjectTagging {};
        let request = Request::new(self, path, command);
        let result = request.execute()?;

        let tagging = if result.1 == 200 {
            let result_string = String::from_utf8_lossy(&result.0);
            println!("{}", result_string);
            Some(serde_xml::deserialize(result_string.as_bytes())?)
        } else {
            None
        };

        Ok((tagging, result.1))
    }

    fn _list(&self,
             prefix: &str,
             delimiter: Option<&str>,
             continuation_token: Option<&str>)
             -> S3Result<(ListBucketResult, u32)> {
        let command = Command::ListBucket {
            prefix,
            delimiter,
            continuation_token,
        };
        let request = Request::new(self, "/", command);
        let result = request.execute()?;
        let result_string = String::from_utf8_lossy(&result.0);
        let deserialized: ListBucketResult = serde_xml::deserialize(result_string.as_bytes())?;
        Ok((deserialized, result.1))
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
    /// let results = bucket.list("/", Some("/")).unwrap();
    /// for (list, code) in results {
    ///     assert_eq!(200, code);
    ///     println!("{:?}", list);
    /// }
    /// ```
    pub fn list(&self, prefix: &str, delimiter: Option<&str>) -> S3Result<Vec<(ListBucketResult, u32)>> {
        let mut results = Vec::new();
        let mut result = self._list(prefix, delimiter, None)?;
        loop {
            results.push(result.clone());
            match result.0.next_continuation_token {
                Some(token) => result = self._list(prefix, delimiter, Some(&token))?,
                None => break,
            }
        }

        Ok(results)
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
