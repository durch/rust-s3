extern crate time;
extern crate curl;
extern crate rustc_serialize;
extern crate openssl;
extern crate url;
#[macro_use]
extern crate log;

use url::Url;
use std::str;
use std::io::prelude::*;

use curl::easy::{Easy, List};

use openssl::crypto::{hmac, hash};
use rustc_serialize::base64::{ToBase64, STANDARD};
use rustc_serialize::hex::ToHex;

macro_rules! get_set {
  ($x:ident, $name:ident) => {
    pub fn $name(&mut self, value: &str) {
      self.$x = value.to_string()
    }
    pub fn $x(&self) -> &str {
      &self.$x
    }
  }
}

macro_rules! link_get_set {
  ($x:ident, $name:ident) => {
    pub fn $name(&mut self, value: &str) {
      self.mut_link().$x = value.to_string()
    }

    pub fn $x(&self) -> &str {
      &self.link().$x
    }
  }
}

/// Struct holding relevant request information so that `Bucket` is a bit cleaner.
pub struct Link {
  long_date: String,
  short_date: String,
  amz_expire: String,
  amz_algo: String,
  amz_region: String,
  amz_service: String,
  amz_payload: String,
  amz_req_ver: String,
  protocol: String
}

impl Link {
  /// Instantiate `Link` with default values, no need to actually change anything here, perhaps only `amz_expire` field.
  pub fn default() -> Link {
    Link {
      long_date: "%Y%m%dT%H%M%SZ".to_string(),
      short_date: "%Y%m%d".to_string(),
      amz_expire: "604800".to_string(),
      amz_algo: "AWS4-HMAC-SHA256".to_string(),
      amz_region: "eu-west-1".to_string(),
      amz_service: "s3".to_string(),
      amz_payload: "UNSIGNED-PAYLOAD".to_string(),
      amz_req_ver: "aws4_request".to_string(),
      protocol: "https".to_string()
    }
  }

  pub fn new(long_date: &str,
             short_date: &str,
             amz_expire: &str,
             amz_algo: &str,
             amz_region: &str,
             amz_service: &str,
             amz_payload: &str,
             amz_req_ver: &str,
             protocol: &str) -> Link {
    Link {
      long_date: long_date.to_string(),
      short_date: short_date.to_string(),
      amz_expire: amz_expire.to_string(),
      amz_algo: amz_algo.to_string(),
      amz_region: amz_region.to_string(),
      amz_service: amz_service.to_string(),
      amz_payload: amz_payload.to_string(),
      amz_req_ver: amz_req_ver.to_string(),
      protocol: protocol.to_string()
    }
  }

  get_set!(long_date, set_long_date);
  get_set!(short_date, set_short_date);
  get_set!(amz_expire, set_amz_expire);
  get_set!(amz_algo, set_amz_algo);
  get_set!(amz_region, set_amz_region);
  get_set!(amz_service, set_amz_service);
  get_set!(amz_payload, set_amz_payload);
  get_set!(amz_req_ver, set_amz_req_ver);
  get_set!(protocol, set_protocol);
}

macro_rules! build_headers {
  ($list:ident, $($x:expr),+) => (
    $($list.append($x).unwrap())+
  )
}

macro_rules! unwrap_get {
  ($get:ident) => (
    match $get {
      Some(x) => {
        if x.len() > 0 {
          x
        } else {
          panic!("These are not the droids you are looking for.")
        }
      },
      None => unreachable!()
    }
  )
}

macro_rules! headers {
  ($transfer:ident, $headers:ident) => (
    $transfer.header_function(|header| {
      $headers.push(str::from_utf8(header).unwrap().to_string());
      true
    }).unwrap();
  )
}

enum Command<'a> {
  Put {
    content: &'a [u8]
  },
  Get,
  List {
    prefix: &'a str,
    delimiter: &'a str
  },
  Delete
}

/// Bucket object for holding info about an S3 bucket
///
/// # Example
/// ```
/// use s3::Bucket;
///
/// let s3_bucket = &"rust-s3-test";
/// let aws_access = &"access_key";
/// let aws_secret = &"secret_key";
///
/// let bucket = Bucket::new(
///               s3_bucket.to_string(),
///               None,
///               aws_access.to_string(),
///               aws_secret.to_string(),
///               None);
/// ```
pub struct Bucket {
    name: String,
    region: Option<String>,
    access_key: String,
    secret_key: String,
    link: Link
}


impl Bucket {
  /// Instantiate a new `Bucket`, in case `Link` is not provided a `Link::default()` is generated.
  pub fn new(name: String,
             region: Option<String>,
             access_key: String,
             secret_key: String,
             link: Option<Link>) -> Bucket {
      Bucket {
          name: name,
          region: region,
          access_key: access_key,
          secret_key: secret_key,
          link: match link {
                  Some(x) => x,
                  None => Link::default()
                }
      }
  }

  pub fn link(&self) -> &Link {
    &self.link
  }

  pub fn mut_link(&mut self) -> &mut Link {
    &mut self.link
  }

  pub fn set_link(&mut self, link: Link) {
    self.link = link
  }

  link_get_set!(long_date, set_long_date);
  link_get_set!(short_date, set_short_date);
  link_get_set!(amz_expire, set_amz_expire);
  link_get_set!(amz_algo, set_amz_algo);
  link_get_set!(amz_region, set_amz_region);
  link_get_set!(amz_service, set_amz_service);
  link_get_set!(amz_payload, set_amz_payload);
  link_get_set!(amz_req_ver, set_amz_req_ver);
  link_get_set!(protocol, set_protocol);

  fn execute(&self,
               cmd: Command,
               path: &str ) -> (String, Option<Vec<u8>>) {

    let content_type = "text/plain";

    let path = if path.starts_with("/") {&path[1..]} else {path};
    debug!("s3_path: {}", path);

    let host = self.host();
    debug!("host: {}", host);

    let date = time::now_utc().rfc822z().to_string();
    debug!("date: {}", date);

    let cmd_string = match cmd {
      Command::Put { content } => "PUT",
      _ => "GET"
    };

    debug!("command: {}", cmd_string);
    let a = self.auth(&cmd_string, &date, path, "", &content_type);
    debug!("auth: {}", a);

    // TODO implement delimiter into request for List
    let url = match cmd {
      Command::List { prefix, delimiter } => format!("{}://{}/?prefix={}", &self.link.protocol, host, prefix),
      _ => format!("{}://{}/{}", &self.link.protocol, host, path)
    };
    debug!("url: {}", url);

    let mut handle = Easy::new();
    let _ = handle.url(&url[..]);

    let mut list = List::new();

    let l = match cmd {
      Command::Put { content } => content.len(),
      _ => 0
    };

    build_headers!(list,
            &format!("Host: {}", &host),
            &format!("Date: {}", &date),
            &format!("Authorization: {}", &a),
            &format!("Content-Type: {}", &content_type),
            &format!("Content-Length: {}", l));

    for l in list.iter(){
      debug!("{:?}", String::from_utf8_lossy(l));
    }

    match cmd {
      Command::Put { content } => {
        handle.put(true).unwrap();
        let _ = handle.post_field_size(l as u64);
      }
      Command::Delete => {
        handle.custom_request(&"DELETE").unwrap()
      }
      _ => {}
    }

    handle.http_headers(list).unwrap();

    let result = match cmd {
                  Command::Put { content } => self.put(&mut handle, content),
                  Command::Get => self.get(&mut handle),
                  Command::List { delimiter, prefix }=> self.list(&mut handle),
                  Command::Delete => self.get(&mut handle)
                };
    (url, result)
  }

  pub fn put(&self, handle: &mut Easy, mut content: &[u8]) -> Option<Vec<u8>> {
    let mut headers = Vec::new();
    {
      let mut transfer = handle.transfer();

      transfer.read_function(|buf| {
          Ok(content.read(buf).unwrap())
      }).unwrap();

      headers!(transfer, headers);

      transfer.perform().unwrap();
    }
    debug!("recieved headers: {:?}", headers);
    debug!("response: {}", handle.response_code().unwrap());
    None
  }

  pub fn get(&self, handle: &mut Easy) -> Option<Vec<u8>> {
    let mut headers = Vec::new();
    let mut dst = Vec::new();

    {
      let mut transfer = handle.transfer();
      transfer.write_function(|data| {
          dst.extend_from_slice(data);
          Ok(data.len())
      }).unwrap();

      headers!(transfer, headers);

      transfer.perform().unwrap();
    }
    debug!("recieved headers: {:?}", headers);
    Some(dst)
  }

  pub fn list(&self, handle: &mut Easy) -> Option<Vec<u8>> {
    let mut dst = Vec::new();
    let mut headers = Vec::new();
    {
      let mut transfer = handle.transfer();
      transfer.write_function(|data| {
          dst.extend_from_slice(data);
          Ok(data.len())
      }).unwrap();

      headers!(transfer, headers);

      transfer.perform().unwrap();
    }
    Some(dst)
  }

  pub fn host(&self) -> String {
      format!("{}.s3{}.amazonaws.com", self.name,
              match self.region {
                  Some(ref r) => format!("-{}", r),
                  None => String::new(),
              })
  }

  fn auth(&self, verb: &str, date: &str, path: &str,
          md5: &str, content_type: &str) -> String {
      let string = format!("{verb}\n{md5}\n{ty}\n{date}\n{headers}{resource}",
                           verb = verb,
                           md5 = md5,
                           ty = content_type,
                           date = date,
                           headers = "",
                           resource = format!("/{}/{}", self.name, path));
      let signature = {
          let mut hmac = hmac::HMAC::new(hash::Type::SHA1, self.secret_key.as_bytes());
          let _ = hmac.write_all(string.as_bytes());
          hmac.finish().to_base64(STANDARD)
      };
      format!("AWS {}:{}", self.access_key, signature)
  }
}

/// Gets file from an S3 path
///
/// # Example:
///
/// ```
/// use s3::{Bucket, get_s3};
/// use std::io::prelude::*;
/// use std::fs::File;
///
/// let s3_bucket = &"rust-s3-test";
/// let aws_access = &"access_key";
/// let aws_secret = &"secret_key";
///
/// let bucket = Bucket::new(
///               s3_bucket.to_string(),
///               None,
///               aws_access.to_string(),
///               aws_secret.to_string(),
///               None);
/// let path = &"test.file";
/// let mut buffer = match File::create(path) {
///           Ok(x) => x,
///           Err(e) => panic!("{:?}, {}", e, path)
///         };
/// let bytes = get_s3(&bucket, Some(&path));
/// match buffer.write(&bytes) {
///   Ok(_) => {} // info!("Written {} bytes from {}", x, path),
///   Err(e) => panic!("{:?}", e)
/// }
/// ```
pub fn get_s3(bucket: &Bucket, s3_path: Option<&str>) -> Vec<u8> {
      let path = match s3_path {
        Some(x) => x,
        None => "/"
      };
      let (_, result) = bucket.execute(Command::Get, path);
      unwrap_get!(result)
    }

/// Delete file from an S3 path
///
/// # Example:
///
/// ```
/// use s3::{Bucket, delete_s3};
/// use std::io::prelude::*;
/// use std::fs::File;
///
/// let s3_bucket = &"rust-s3-test";
/// let aws_access = &"access_key";
/// let aws_secret = &"secret_key";
///
/// let bucket = Bucket::new(
///               s3_bucket.to_string(),
///               None,
///               aws_access.to_string(),
///               aws_secret.to_string(),
///               None);
/// let path = &"test.file";
/// let mut buffer = match File::create(path) {
///           Ok(x) => x,
///           Err(e) => panic!("{:?}, {}", e, path)
///         };
/// delete_s3(&bucket, &path);
///
/// ```
pub fn delete_s3(bucket: &Bucket, s3_path: &str) {
      let (_, _) = bucket.execute(Command::Delete, s3_path);
    }

/// List contents of an S3 bucket, `prefix` and `delimiter` are placeholders for now
///
/// # Example
///
/// ```
/// use s3::{Bucket, list_s3};
/// use std::io::prelude::*;
/// use std::fs::File;
///
/// let s3_bucket = &"rust-s3-test";
/// let aws_access = &"access_key";
/// let aws_secret = &"secret_key";
///
/// let bucket = Bucket::new(
///               s3_bucket.to_string(),
///               None,
///               aws_access.to_string(),
///               aws_secret.to_string(),
///               None);
/// let bytes = list_s3(&bucket,
///                       &"/",
///                       &"/",
///                       &"/");
/// let string = String::from_utf8_lossy(&bytes);
/// println!("{}", string);
/// ```
pub fn list_s3(bucket: &Bucket, path: &str, prefix: &str, delimiter: &str) -> Vec<u8> {
      // TODO prefix + delimiter support, default delimiter is / ATM
      let (_, result) = bucket.execute(Command::List {
                                          prefix: prefix,
                                          delimiter: delimiter
                                        },
                                        path);
      unwrap_get!(result)
    }

fn sign(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut hmac = hmac::HMAC::new(hash::Type::SHA256, key);
    let _ = hmac.write_all(msg);
    hmac.finish()
  }

fn get_signature_key(key: &str, date_stamp: &str, region_name: &str, service_name: &str, version: &str) -> Vec<u8> {
  let kdate = sign(format!("AWS4{}", key).as_bytes(), date_stamp.as_bytes());
  let kregion = sign(kdate.as_slice(), region_name.as_bytes());
  let kservice = sign(kregion.as_slice(), service_name.as_bytes());
  sign(&kservice, version.as_bytes())
}

/// Put into an S3 bucket, get a preauthorized link back.
///
/// # Example
///
/// ```
/// use s3::{Bucket, put_s3};
///
/// let s3_bucket = &"rust-s3-test";
/// let aws_access = &"access_key";
/// let aws_secret = &"secret_key";
///
/// let bucket = Bucket::new(
///               s3_bucket.to_string(),
///               None,
///               aws_access.to_string(),
///               aws_secret.to_string(),
///               None);
/// let put_me = "I want to go to S3".to_string();
/// let url = put_s3(&bucket,
///                 &"/test.file",
///                 &put_me.as_bytes());
/// println!("{}", url);
/// ```
pub fn put_s3(bucket: &Bucket,
              s3_path: &str,
              output: &[u8]) -> String {

  let (url, _) = bucket.execute(Command::Put { content: output }, &s3_path);
  let t = time::now();
  let method = &"GET";
  let amzdate = match t.strftime(&bucket.link.long_date) {
    Ok(x) => x.to_string(),
    Err(e) => panic!("{:?}", e)
  };
  let datestamp = match t.strftime(&bucket.link.short_date) {
    Ok(x) => x.to_string(),
    Err(e) => panic!("{:?}", e)
  };
  let url: Url = match url.parse() {
    Ok(x) => x,
    Err(e) => panic!("{:?}", e)
  };
  let canonical_uri = &url.path();
  let host = match url.host_str() {
    Some(x) => x,
    None => panic!("Unable to extract host string from url: {:?}", url)
  };
  let endpoint = format!("https://{}", host);
  let canonical_headers = format!("host:{}\n", host);
  let signed_headers = &"host";
  let payload_hash = &bucket.link.amz_payload;
  let credential_scope = format!("{}/{}/{}/{}",
                                 datestamp,
                                 &bucket.link.amz_region,
                                 &bucket.link.amz_service,
                                 &bucket.link.amz_req_ver);
  let canonical_querystring = format!("{}={}&{}={}%2F{}&{}={}&{}={}&{}={}",
                                &"X-Amz-Algorithm", &bucket.link.amz_algo,
                                &"X-Amz-Credential", bucket.access_key, credential_scope.replace("/", "%2F"),
                                &"X-Amz-Date", &amzdate,
                                &"X-Amz-Expires", bucket.link.amz_expire,
                                &"X-Amz-SignedHeaders", signed_headers);
  let canonical_request = format!("{}\n{}\n{}\n{}\n{}\n{}",
                                  method,
                                  &canonical_uri,
                                  canonical_querystring,
                                  canonical_headers,
                                  signed_headers,
                                  payload_hash);
  let string_to_sign = format!("{}\n{}\n{}\n{}",
                           bucket.link.amz_algo,
                           amzdate,
                           credential_scope,
                           hash::hash(hash::Type::SHA256, canonical_request.as_bytes()).to_hex());
  let signing_key = get_signature_key(&bucket.secret_key,
                                      &datestamp,
                                      &bucket.link.amz_region,
                                      &bucket.link.amz_service,
                                      &bucket.link.amz_req_ver);
  let signature = sign(&signing_key, &string_to_sign.as_bytes()).to_hex();
  let canonical_querystring = format!("{}&{}={}",
                                      canonical_querystring,
                                      &"X-Amz-Signature", signature);
  format!("{}{}?{}",
                endpoint,
                canonical_uri,
                canonical_querystring)
}

