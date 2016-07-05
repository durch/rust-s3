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

pub const LONG_DATE: &'static str = "%Y%m%dT%H%M%SZ";
const SHORT_DATE: &'static str = "%Y%m%d";
const AMZ_EXPIRE: &'static str = "604800";
const AMZ_ALGO: &'static str = "AWS4-HMAC-SHA256";
const AMZ_REGION: &'static str = "eu-west-1";
const AMZ_SERVICE: &'static str = "s3";
const AMZ_PAYLOAD: &'static str = "UNSIGNED-PAYLOAD";
const AMZ_REQ_VER: &'static str = "aws4_request";


pub struct Bucket {
    name: String,
    region: Option<String>,
    access_key: String,
    secret_key: String,
    proto: String,
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
  }
}

impl Bucket {
  pub fn new(name: String,
             region: Option<String>,
             access_key: String,
             secret_key: String,
             proto: &str) -> Bucket {
      Bucket {
          name: name,
          region: region,
          access_key: access_key,
          secret_key: secret_key,
          proto: proto.to_string(),
      }
  }

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
      Command::List { prefix, delimiter } => format!("{}://{}/?prefix={}", self.proto, host, prefix),
      _ => format!("{}://{}/{}", self.proto, host, path)
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
      _ => {}
    }

    handle.http_headers(list).unwrap();

    let result = match cmd {
                  Command::Put { content } => self.put(&mut handle, content),
                  Command::Get => self.get(&mut handle),
                  Command::List { delimiter, prefix }=> self.list(&mut handle)
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

    // TODO implement DELETE Command

    // pub fn delete<'a, 'b>(&self, handle: &'a mut http::Handle, path: &str)
    //                       -> http::Request<'a, 'b> {
    //     let path = if path.starts_with("/") {&path[1..]} else {path};
    //     let host = self.host();
    //     let date = time::now().rfc822z().to_string();
    //     let auth = self.auth("DELETE", &date, path, "", "");
    //     let url = format!("{}://{}/{}", self.proto, host, path);
    //     handle.delete(&url[..])
    //           .header("Host", &host)
    //           .header("Date", &date)
    //           .header("Authorization", &auth)
    // }

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

pub fn get_s3(bucket: &Bucket, s3_path: Option<&str>) -> Vec<u8> {
      let path = match s3_path {
        Some(x) => x,
        None => "/"
      };
      let (_, result) = bucket.execute(Command::Get, path);
      unwrap_get!(result)
    }

// TODO prefix + delimiter support, default delimiter is / ATM
pub fn list_s3(bucket: &Bucket, path: &str, prefix: &str, delimiter: &str) -> Vec<u8> {
      let (_, result) = bucket.execute(Command::List { prefix: prefix, delimiter: delimiter }, path);
      unwrap_get!(result)
    }

fn sign(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut hmac = hmac::HMAC::new(hash::Type::SHA256, key);
    let _ = hmac.write_all(msg);
    hmac.finish()
  }

fn get_signature_key(key: &str, date_stamp: &str, region_name: &str, service_name: &str) -> Vec<u8> {
  let kdate = sign(format!("AWS4{}", key).as_bytes(), date_stamp.as_bytes());
  let kregion = sign(kdate.as_slice(), region_name.as_bytes());
  let kservice = sign(kregion.as_slice(), service_name.as_bytes());
  sign(&kservice, AMZ_REQ_VER.as_bytes())
}

pub fn put_s3(bucket: &Bucket,
              s3_path: &str,
              output: &[u8]) -> String {

  let (url, _) = bucket.execute(Command::Put { content: output }, &s3_path);
  let t = time::now();
  let method = &"GET";
  let amzdate = match t.strftime(LONG_DATE) {
    Ok(x) => x.to_string(),
    Err(e) => panic!("{:?}", e)
  };
  let datestamp = match t.strftime(SHORT_DATE) {
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
  let payload_hash = AMZ_PAYLOAD;
  let credential_scope = format!("{}/{}/{}/{}",
                                 datestamp,
                                 AMZ_REGION,
                                 AMZ_SERVICE,
                                 AMZ_REQ_VER);
  let canonical_querystring = format!("{}={}&{}={}%2F{}&{}={}&{}={}&{}={}",
                                &"X-Amz-Algorithm", AMZ_ALGO,
                                &"X-Amz-Credential", bucket.access_key, credential_scope.replace("/", "%2F"),
                                &"X-Amz-Date", &amzdate,
                                &"X-Amz-Expires", AMZ_EXPIRE,
                                &"X-Amz-SignedHeaders", signed_headers);
  let canonical_request = format!("{}\n{}\n{}\n{}\n{}\n{}",
                                  method,
                                  &canonical_uri,
                                  canonical_querystring,
                                  canonical_headers,
                                  signed_headers,
                                  payload_hash);
  let string_to_sign = format!("{}\n{}\n{}\n{}",
                           AMZ_ALGO,
                           amzdate,
                           credential_scope,
                           hash::hash(hash::Type::SHA256, canonical_request.as_bytes()).to_hex());
  let signing_key = get_signature_key(&bucket.secret_key, &datestamp, AMZ_REGION, AMZ_SERVICE);
  let signature = sign(&signing_key, &string_to_sign.as_bytes()).to_hex();
  let canonical_querystring = format!("{}&{}={}",
                                      canonical_querystring,
                                      &"X-Amz-Signature", signature);
  format!("{}{}?{}",
                endpoint,
                canonical_uri,
                canonical_querystring)
}

