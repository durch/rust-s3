//! Implementation of [AWS V4 Signing][link]
//!
//! [link]: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

use std::str;

use chrono::{DateTime, Utc};
use hex::ToHex;
use hmac::{Hmac, Mac};
use url::Url;
use region::Region;
use request::Headers;
use sha2::{Digest, Sha256};

use serde_xml;
use serde_types::ListBucketResult;

const SHORT_DATE: &'static str = "%Y%m%d";
const LONG_DATETIME: &'static str = "%Y%m%dT%H%M%SZ";

/// Encode a URI following the specific requirements of the AWS service.
pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    let mut result = String::with_capacity(string.len() * 2);
    for c in string.chars() {
        match c {
            'a'...'z' | 'A'...'Z' | '0'...'9' | '_' | '-' | '~' | '.' => result.push(c),
            '/' if encode_slash => result.push_str("%2F"),
            '/' if !encode_slash => result.push('/'),
            _ => {
                result.push('%');
                result.push_str(&format!("{}", c)
                    .bytes()
                    .map(|b| format!("{:02X}", b))
                    .collect::<String>());
            }
        }
    }
    result
}

/// Generate a canonical URI string from the given URL.
pub fn canonical_uri_string(uri: &Url) -> String {
    uri.path().into()
}

/// Generate a canonical query string from the query pairs in the given URL.
pub fn canonical_query_string(uri: &Url) -> String {
    let mut keyvalues = uri.query_pairs()
        .map(|(key, value)| uri_encode(&key, true) + "=" + &uri_encode(&value, true))
        .collect::<Vec<String>>();
    keyvalues.sort();
    keyvalues.join("&")
}

/// Generate a canonical header string from the provided headers.
pub fn canonical_header_string(headers: &Headers) -> String {
    let mut keyvalues = headers.iter()
        .map(|(key, value)| key.to_lowercase() + ":" + value.trim())
        .collect::<Vec<String>>();
    keyvalues.sort();
    keyvalues.join("\n")
}

/// Generate a signed header string from the provided headers.
pub fn signed_header_string(headers: &Headers) -> String {
    let mut keys = headers.iter().map(|(key, _)| key.to_lowercase()).collect::<Vec<String>>();
    keys.sort();
    keys.join(";")
}

/// Generate a canonical request.
pub fn canonical_request(method: &str, url: &Url, headers: &Headers, sha256: &str) -> String {
    format!("{method}\n{uri}\n{query_string}\n{headers}\n\n{signed}\n{sha256}",
            method = method,
            uri = canonical_uri_string(&url),
            query_string = canonical_query_string(&url),
            headers = canonical_header_string(headers),
            signed = signed_header_string(&headers),
            sha256 = sha256)
}

/// Generate an AWS scope string.
pub fn scope_string(datetime: &DateTime<Utc>, region: Region) -> String {
    format!("{date}/{region}/s3/aws4_request",
            date = datetime.format(SHORT_DATE),
            region = region)
}

/// Generate the "string to sign" - the value to which the HMAC signing is
/// applied to sign requests.
pub fn string_to_sign(datetime: &DateTime<Utc>, region: Region, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.input(canonical_req.as_bytes());
    format!("AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hash}",
            timestamp = datetime.format(LONG_DATETIME),
            scope = scope_string(datetime, region),
            hash = hasher.result().as_slice().to_hex())
}

/// Generate the AWS signing key, derived from the secret key, date, region,
/// and service name.
pub fn signing_key(datetime: &DateTime<Utc>,
                   secret_key: &str,
                   region: Region,
                   service: &str)
                   -> Vec<u8> {
    let secret = String::from("AWS4") + secret_key;
    let mut date_hmac = Hmac::<Sha256>::new(secret.as_bytes());
    date_hmac.input(datetime.format(SHORT_DATE).to_string().as_bytes());
    let mut region_hmac = Hmac::<Sha256>::new(&date_hmac.result().code());
    region_hmac.input(region.to_string().as_bytes());
    let mut service_hmac = Hmac::<Sha256>::new(&region_hmac.result().code());
    service_hmac.input(service.as_bytes());
    let mut signing_hmac = Hmac::<Sha256>::new(&service_hmac.result().code());
    signing_hmac.input("aws4_request".as_bytes());
    signing_hmac.result().code().into()
}

/// Generate the AWS authorization header.
pub fn authorization_header(access_key: &str,
                            datetime: &DateTime<Utc>,
                            region: Region,
                            signed_headers: &str,
                            signature: &str)
                            -> String {
    format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},\
            SignedHeaders={signed_headers},Signature={signature}",
            access_key = access_key,
            scope = scope_string(datetime, region),
            signed_headers = signed_headers,
            signature = signature)
}

#[cfg(test)]
mod tests {
    use std::str;

    use chrono::{TimeZone, Utc};
    use hex::ToHex;
    use url::Url;

    use request::Headers;
    use super::*;

    #[test]
    fn test_base_url_encode() {
        // Make sure parsing doesn't remove extra slashes, as normalization
        // will mess up the path lookup.
        let url = Url::parse("http://s3.amazonaws.com/examplebucket///foo//bar//baz").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!("/examplebucket///foo//bar//baz", canonical);
    }

    #[test]
    fn test_query_string_encode() {
        let url = Url::parse("http://s3.amazonaws.com/examplebucket?\
                              prefix=somePrefix&marker=someMarker&max-keys=20")
            .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("marker=someMarker&max-keys=20&prefix=somePrefix", canonical);

        let url = Url::parse("http://s3.amazonaws.com/examplebucket?acl").unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("acl=", canonical);

        let url = Url::parse("http://s3.amazonaws.com/examplebucket?\
                              key=with%20space&also+space=with+plus")
            .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("also%20space=with%20plus&key=with%20space", canonical);
    }

    #[test]
    fn test_headers_encode() {
        let headers: Headers = vec![("X-Amz-Date".into(), "20130708T220855Z".into()),
                                    ("FOO".into(), "bAr".into()),
                                    ("host".into(), "s3.amazonaws.com".into())]
            .into_iter()
            .collect();
        let canonical = canonical_header_string(&headers);
        let expected = "foo:bAr\nhost:s3.amazonaws.com\nx-amz-date:20130708T220855Z";
        assert_eq!(expected, canonical);

        let signed = signed_header_string(&headers);
        assert_eq!("foo;host;x-amz-date", signed);
    }

    #[test]
    fn test_aws_signing_key() {
        let key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let expected = "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9";
        let datetime = Utc.ymd(2015, 8, 30).and_hms(0, 0, 0);
        let signature = signing_key(&datetime, key, "us-east-1".parse().unwrap(), "iam");
        assert_eq!(expected, signature.to_hex());
    }

    const EXPECTED_SHA: &'static str = "e3b0c44298fc1c149afbf4c8996fb924\
                                        27ae41e4649b934ca495991b7852b855";

    #[cfg_attr(rustfmt, rustfmt_skip)]
    const EXPECTED_CANONICAL_REQUEST: &'static str =
        "GET\n\
         /test.txt\n\
         \n\
         host:examplebucket.s3.amazonaws.com\n\
         range:bytes=0-9\n\
         x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
         x-amz-date:20130524T000000Z\n\
         \n\
         host;range;x-amz-content-sha256;x-amz-date\n\
         e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[cfg_attr(rustfmt, rustfmt_skip)]
    const EXPECTED_STRING_TO_SIGN: &'static str =
        "AWS4-HMAC-SHA256\n\
         20130524T000000Z\n\
         20130524/us-east-1/s3/aws4_request\n\
         7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";

    #[test]
    fn test_signing() {
        let url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let headers: Headers = vec![("X-Amz-Date".into(), "20130524T000000Z".into()),
                                    ("range".into(), "bytes=0-9".into()),
                                    ("host".into(), "examplebucket.s3.amazonaws.com".into()),
                                    ("X-Amz-Content-Sha256".into(), EXPECTED_SHA.into())]
            .into_iter()
            .collect();
        let canonical = canonical_request("GET", &url, &headers, EXPECTED_SHA);
        assert_eq!(EXPECTED_CANONICAL_REQUEST, canonical);

        let datetime = Utc.ymd(2013, 5, 24).and_hms(0, 0, 0);
        let string_to_sign = string_to_sign(&datetime, "us-east-1".parse().unwrap(), &canonical);
        assert_eq!(EXPECTED_STRING_TO_SIGN, string_to_sign);

        let expected = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let signing_key = signing_key(&datetime, secret, "us-east-1".parse().unwrap(), "s3");
        let mut hmac = Hmac::<Sha256>::new(&signing_key);
        hmac.input(string_to_sign.as_bytes());
        assert_eq!(expected, hmac.result().code().to_hex());
    }

    #[test]
    fn test_parse_list_bucket_result() {
        let result_string = r###"
            <?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult
                xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>RelationalAI</Name>
                <Prefix>/</Prefix>
                <KeyCount>0</KeyCount>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>true</IsTruncated>
            </ListBucketResult>
        "###;
        let deserialized: ListBucketResult = serde_xml::deserialize(result_string.as_bytes()).expect("Parse error!");
        assert!(deserialized.is_truncated);
    }
}
