//! Simple access to Amazon Web Service's (AWS) Simple Storage Service (S3)
#![warn(missing_docs)]

extern crate chrono;
extern crate crypto;
extern crate curl;
extern crate hex;
extern crate serde;
extern crate serde_xml;
extern crate url;

#[macro_use]
extern crate log;

pub mod signing;

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Cursor};
use std::mem;
use std::str::{self, FromStr};

use chrono::{DateTime, UTC};
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;
use curl::easy::{Easy, List, ReadError};
use hex::ToHex;
use url::Url;

const LONG_DATE: &'static str = "%Y%m%dT%H%M%SZ";
const EMPTY_PAYLOAD_SHA: &'static str = "e3b0c44298fc1c149afbf4c8996fb924\
                                         27ae41e4649b934ca495991b7852b855";

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

/// Object holding info about an S3 bucket which provides easy access to S3
/// operations.
///
/// # Example
/// ```
/// use s3::{Bucket, Credentials};
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::new("access_key", "secret_key", None);
///
/// let bucket = Bucket::new(bucket_name, region, credentials);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    name: String,
    region: Region,
    credentials: Credentials,
    extra_headers: Headers,
    extra_query: Query,
    endpoint: Option<String>,
}

/// AWS access credentials: access key, secret key, and optional token.
///
/// # Example
/// ```
/// use s3::Credentials;
///
/// // Load from environment AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
/// // AWS_SESSION_TOKEN variables
/// // TODO let credentials = Credentials::from_env().unwrap();
///
/// // Load credentials from the standard AWS credentials file with the given
/// // profile name.
/// // TODO let credentials = Credentials::from_profile("default").unwrap();
///
/// // Initialize directly with key ID, secret key, and optional token
/// let credentials = Credentials::new("access_key", "secret_key", Some("token"));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Credentials {
    /// AWS public access key.
    pub access_key: String,
    /// AWS secret key.
    pub secret_key: String,
    /// Temporary token issued by AWS service.
    pub token: Option<String>,
    _private: (),
}

/// AWS S3 [region identifier](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)
///
/// # Example
/// ```
/// use std::str::FromStr;
/// use s3::Region;
///
/// // Parse from a string
/// let region: Region = "us-east-1".parse().unwrap();
///
/// // Choose region directly
/// let region = Region::EuWest2;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Region {
    /// us-east-1
    UsEast1,
    /// us-east-2
    UsEast2,
    /// us-west-1
    UsWest1,
    /// us-west-2
    UsWest2,
    /// ca-central-1
    CaCentral1,
    /// ap-south-1
    ApSouth1,
    /// ap-northeast-1
    ApNortheast1,
    /// ap-northeast-2
    ApNortheast2,
    /// ap-southeast-1
    ApSoutheast1,
    /// ap-southeast-2
    ApSoutheast2,
    /// eu-central-1
    EuCentral1,
    /// eu-west-1
    EuWest1,
    /// eu-west-2
    EuWest2,
    /// sa-east-1
    SaEast1,
}

/// Error raised when a string cannot be parsed to a valid
/// [`Region`](enum.Region.html).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3RegionParseError(String);

/// Errors raised by the libray when performing generic operations.
#[derive(Debug)]
pub enum S3Error {
    /// Error occurred during transfer/communication with the S3 HTTP/HTTPS
    /// endpoint.
    CurlError(curl::Error),
    /// General Input/Output error.
    IoError(io::Error),
    /// Unable to decode the XML returned from S3.
    XmlError(serde_xml::Error),
    /// Unable to construct a url with the endpoint, bucket name and path.
    UrlParseError(url::ParseError),
}

/// Generic return type of S3 functions.
pub type S3Result<T> = Result<T, S3Error>;

/// Collection of HTTP headers sent to S3 service, in key/value format.
pub type Headers = HashMap<String, String>;

/// Collection of HTTP query parameters sent to S3 service, in key/value
/// format.
pub type Query = HashMap<String, String>;

enum Command<'a> {
    Put {
        content: &'a [u8],
        content_type: &'a str,
    },
    Get,
    Delete,
    List {
        prefix: &'a str,
        delimiter: Option<&'a str>,
    },
}

// Temporary structure for making a request
struct Request<'a> {
    bucket: &'a Bucket,
    path: &'a str,
    command: Command<'a>,
    datetime: DateTime<UTC>,
}

impl Bucket {
    /// Instantiate a new `Bucket`.
    ///
    /// # Example
    /// ```
    /// use s3::{Bucket, Credentials};
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
            endpoint: None,
        }
    }

    /// Gets file from an S3 path.
    ///
    /// # Example:
    ///
    /// ```rust,no_run
    /// use s3::{Bucket, Credentials};
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
    /// use s3::{Bucket, Credentials};
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
    /// use s3::{Bucket, Credentials};
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
    /// use s3::{Bucket, Credentials};
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
        let result = try!(request.execute());
        let result_string = String::from_utf8_lossy(&result.0);
        let deserialized: ListBucketResult = try!(serde_xml::from_str(&result_string));
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

    /// Get the custom endpoint if one has been set.
    pub fn get_endpoint(&self) -> Option<&str> {
        self.endpoint.as_ref().map(|e| e.as_str())
    }

    /// Sets the endpoint to be used.
    /// If set to None (default), the endpoint will be constructed via the region.
    pub fn set_endpoint(&mut self, endpoint: Option<String>) {
        self.endpoint = endpoint;
    }
}

impl Credentials {
    /// Initialize Credentials directly with key ID, secret key, and optional
    /// token.
    pub fn new(access_key: &str, secret_key: &str, token: Option<&str>) -> Credentials {
        Credentials {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            token: token.map(|s| s.into()),
            _private: (),
        }
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Region::*;
        match *self {
            UsEast1 => write!(f, "us-east-1"),
            UsEast2 => write!(f, "us-east-2"),
            UsWest1 => write!(f, "us-west-1"),
            UsWest2 => write!(f, "us-west-2"),
            CaCentral1 => write!(f, "ca-central-1"),
            ApSouth1 => write!(f, "ap-south-1"),
            ApNortheast1 => write!(f, "ap-northeast-1"),
            ApNortheast2 => write!(f, "ap-northeast-2"),
            ApSoutheast1 => write!(f, "ap-southeast-1"),
            ApSoutheast2 => write!(f, "ap-southeast-2"),
            EuCentral1 => write!(f, "eu-central-1"),
            EuWest1 => write!(f, "eu-west-1"),
            EuWest2 => write!(f, "eu-west-2"),
            SaEast1 => write!(f, "sa-east-1"),
        }
    }
}

impl FromStr for Region {
    type Err = S3RegionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::Region::*;
        match s {
            "us-east-1" => Ok(UsEast1),
            "us-east-2" => Ok(UsEast2),
            "us-west-1" => Ok(UsWest1),
            "us-west-2" => Ok(UsWest2),
            "ca-central-1" => Ok(CaCentral1),
            "ap-south-1" => Ok(ApSouth1),
            "ap-northeast-1" => Ok(ApNortheast1),
            "ap-northeast-2" => Ok(ApNortheast2),
            "ap-southeast-1" => Ok(ApSoutheast1),
            "ap-southeast-2" => Ok(ApSoutheast2),
            "eu-central-1" => Ok(EuCentral1),
            "eu-west-1" => Ok(EuWest1),
            "eu-west-2" => Ok(EuWest2),
            "sa-east-1" => Ok(SaEast1),
            _ => Err(S3RegionParseError(s.to_string())),
        }
    }
}

impl Region {
    fn endpoint(&self) -> &str {
        use self::Region::*;
        match *self {
            // Surprisingly, us-east-1 does not have a
            // s3-us-east-1.amazonaws.com DNS record
            UsEast1 => "s3.amazonaws.com",
            UsEast2 => "s3-us-east-2.amazonaws.com",
            UsWest1 => "s3-us-west-1.amazonaws.com",
            UsWest2 => "s3-us-west-2.amazonaws.com",
            CaCentral1 => "s3-ca-central-1.amazonaws.com",
            ApSouth1 => "s3-ap-south-1.amazonaws.com",
            ApNortheast1 => "s3-ap-northeast-1.amazonaws.com",
            ApNortheast2 => "s3-ap-northeast-2.amazonaws.com",
            ApSoutheast1 => "s3-ap-southeast-1.amazonaws.com",
            ApSoutheast2 => "s3-ap-southeast-2.amazonaws.com",
            EuCentral1 => "s3-eu-central-1.amazonaws.com",
            EuWest1 => "s3-eu-west-1.amazonaws.com",
            EuWest2 => "s3-eu-west-2.amazonaws.com",
            SaEast1 => "s3-sa-east-1.amazonaws.com",
        }
    }
}

impl<'a> Command<'a> {
    pub fn http_verb(&self) -> &'static str {
        match *self {
            Command::Get => "GET",
            Command::Put { .. } => "PUT",
            Command::Delete => "DELETE",
            Command::List { .. } => "GET",
        }
    }
}

impl<'a> Request<'a> {
    pub fn new<'b>(bucket: &'b Bucket, path: &'b str, command: Command<'b>) -> Request<'b> {
        Request {
            bucket: bucket,
            path: path,
            command: command,
            datetime: UTC::now(),
        }
    }

    fn url(&self) -> S3Result<Url> {
        let mut url_str = self.bucket.endpoint.clone().unwrap_or_else(|| {
            let mut url_str = String::from("https://");
            url_str.push_str(self.bucket.host());
            url_str
        });

        if !url_str.ends_with('/') {
            url_str.push('/');
        }
        url_str.push_str(self.bucket.name());

        if !self.path.starts_with('/') {
            url_str.push('/');
        }
        url_str.push_str(&signing::uri_encode(self.path, false));

        let mut url = try!(Url::parse(&url_str));
        for (key, value) in self.bucket.extra_query.iter() {
            url.query_pairs_mut().append_pair(key, value);
        }

        if let Command::List { prefix, delimiter } = self.command {
            let mut query_pairs = url.query_pairs_mut();
            delimiter.map(|d| query_pairs.append_pair("delimiter", d));
            query_pairs.append_pair("prefix", prefix);
            query_pairs.append_pair("list-type", "2");
        }

        Ok(url)
    }

    fn content_length(&self) -> usize {
        match self.command {
            Command::Put { content, .. } => content.len(),
            _ => 0,
        }
    }

    fn content_type(&self) -> String {
        match self.command {
            Command::Put { content_type, .. } => content_type.into(),
            _ => "text/plain".into(),
        }
    }

    fn sha256(&self) -> String {
        match self.command {
            Command::Put { content, .. } => {
                let mut sha = Sha256::new();
                sha.input(content);
                sha.result_str()
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }

    fn long_date(&self) -> String {
        self.datetime.format(LONG_DATE).to_string()
    }

    fn canonical_request(&self, headers: &Headers) -> S3Result<String> {
        Ok(signing::canonical_request(self.command.http_verb(),
                                      &try!(self.url()),
                                      headers,
                                      &self.sha256()))
    }

    fn string_to_sign(&self, request: &str) -> String {
        signing::string_to_sign(&self.datetime, self.bucket.region(), &request)
    }

    fn signing_key(&self) -> Vec<u8> {
        signing::signing_key(&self.datetime,
                             self.bucket.secret_key(),
                             self.bucket.region(),
                             "s3")
    }

    fn authorization(&self, headers: &Headers) -> S3Result<String> {
        let canonical_request = try!(self.canonical_request(headers));
        let string_to_sign = self.string_to_sign(&canonical_request);
        let mut hmac = Hmac::new(Sha256::new(), &self.signing_key());
        hmac.input(string_to_sign.as_bytes());
        let signature = hmac.result().code().to_hex();
        let signed_header = signing::signed_header_string(headers);
        Ok(signing::authorization_header(self.bucket.access_key(),
                                         &self.datetime,
                                         self.bucket.region(),
                                         &signed_header,
                                         &signature))
    }

    fn headers(&self) -> S3Result<Headers> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.sha256();

        // Start with extra_headers, that way our headers replace anything with
        // the same name.
        let mut headers: Headers = self.bucket.extra_headers.clone();
        headers.insert("Host".into(), self.bucket.host().into());
        headers.insert("Content-Length".into(), self.content_length().to_string());
        headers.insert("Content-Type".into(), self.content_type());
        headers.insert("X-Amz-Content-Sha256".into(), sha256.clone());
        headers.insert("X-Amz-Date".into(), self.long_date());

        self.bucket.credentials().token.as_ref().map(|token| {
            headers.insert("X-Amz-Security-Token".into(), token.clone());
        });

        // This must be last, as it signs the other headers
        let authorization = self.authorization(&headers);
        headers.insert("Authorization".into(), try!(authorization));

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert("Date".into(), self.datetime.to_rfc2822());

        Ok(headers)
    }

    fn load_content(&self, handle: &mut Easy) -> S3Result<Cursor<&[u8]>> {
        if let Command::Put { content, .. } = self.command {
            try!(handle.put(true));
            try!(handle.post_field_size(content.len() as u64));
            Ok(Cursor::new(content))
        } else {
            Ok(Cursor::new(&[] as &[u8]))
        }
    }

    fn execute(&self) -> S3Result<(Vec<u8>, u32)> {
        let mut handle = Easy::new();
        try!(handle.url(try!(self.url()).as_str()));

        // Special handling to load PUT content
        let mut content_cursor = try!(self.load_content(&mut handle));

        // Set GET, PUT, etc
        try!(handle.custom_request(self.command.http_verb()));

        // Build and set a Curl List of headers
        let mut list = List::new();
        for (key, value) in try!(self.headers()).iter() {
            let header = format!("{}: {}", key, value);
            try!(list.append(&header));
        }
        try!(handle.http_headers(list));

        // Run the transfer
        let mut dst = Vec::new();
        {
            let mut transfer = handle.transfer();

            try!(transfer.read_function(|buf| content_cursor.read(buf).or(Err(ReadError::Abort))));

            try!(transfer.write_function(|data| {
                dst.extend_from_slice(data);
                Ok(data.len())
            }));

            try!(transfer.perform());
        }
        Ok((dst, try!(handle.response_code())))
    }
}

impl From<curl::Error> for S3Error {
    fn from(e: curl::Error) -> S3Error {
        S3Error::CurlError(e)
    }
}

impl From<io::Error> for S3Error {
    fn from(e: io::Error) -> S3Error {
        S3Error::IoError(e)
    }
}

impl From<S3Error> for io::Error {
    fn from(e: S3Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
    }
}

impl From<serde_xml::Error> for S3Error {
    fn from(e: serde_xml::Error) -> S3Error {
        S3Error::XmlError(e)
    }
}

impl From<url::ParseError> for S3Error {
    fn from(e: url::ParseError) -> S3Error {
        S3Error::UrlParseError(e)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn region_url(bucket_name: &str, path: &str, region: Region) -> String {
        let credentials = Credentials::new("access_key", "secret_key", None);
        let bucket = Bucket::new(bucket_name, region, credentials);
        let request = Request::new(&bucket, path, Command::Get);
        request.url().unwrap().to_string()
    }

    #[test]
    fn test_region_url() {
        assert_eq!(region_url("some-bucket", "some-path", Region::ApSoutheast1),
                   "https://s3-ap-southeast-1.amazonaws.com/some-bucket/some-path");
        assert_eq!(region_url("foo", "bar", Region::UsEast1),
                   "https://s3.amazonaws.com/foo/bar");
        assert_eq!(region_url("foo", "/bar/baz", Region::UsEast1),
                   "https://s3.amazonaws.com/foo/bar/baz");
    }

    fn endpoint_url(bucket_name: &str, path: &str, endpoint: &str) -> String {
        let credentials = Credentials::new("access_key", "secret_key", None);
        let mut bucket = Bucket::new(bucket_name, Region::UsEast1, credentials);
        bucket.set_endpoint(Some(String::from(endpoint)));
        let request = Request::new(&bucket, path, Command::Get);
        request.url().unwrap().to_string()
    }

    #[test]
    fn test_endpoint_url() {
        assert_eq!(endpoint_url("testbucket", "testpath", "http://localhost/"),
                   "http://localhost/testbucket/testpath");
        assert_eq!(endpoint_url("foo", "/bar/baz", "http://localhost:8080"),
                   "http://localhost:8080/foo/bar/baz");
        assert_eq!(endpoint_url("bucket", "path", "https://mypersonals3.org/"),
                   "https://mypersonals3.org/bucket/path");
        assert_eq!(endpoint_url("bucket", "path", "https://mypersonals3.com:1234/"),
                   "https://mypersonals3.com:1234/bucket/path");
    }
}