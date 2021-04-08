//! Implementation of [AWS V4 Signing][link]
//!
//! [link]: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

use std::str;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac, NewMac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::{Digest, Sha256};
use url::Url;

use crate::region::Region;
use anyhow::anyhow;
use anyhow::Result;
use http::HeaderMap;

const SHORT_DATE: &str = "%Y%m%d";
const LONG_DATETIME: &str = "%Y%m%dT%H%M%SZ";

pub type HmacSha256 = Hmac<Sha256>;

// https://perishablepress.com/stop-using-unsafe-characters-in-urls/
pub const FRAGMENT: &AsciiSet = &CONTROLS
    // URL_RESERVED
    .add(b':')
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b';')
    .add(b'=')
    // URL_UNSAFE
    .add(b'"')
    .add(b' ')
    .add(b'<')
    .add(b'>')
    .add(b'%')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'\\')
    .add(b'^')
    .add(b'`');

pub const FRAGMENT_SLASH: &AsciiSet = &FRAGMENT.add(b'/');

/// Encode a URI following the specific requirements of the AWS service.
#[cfg(not(feature = "never-encode-slash"))]
pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    if encode_slash {
        utf8_percent_encode(string, FRAGMENT_SLASH).to_string()
    } else {
        utf8_percent_encode(string, FRAGMENT).to_string()
    }
}

#[cfg(feature = "never-encode-slash")]
pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    utf8_percent_encode(string, FRAGMENT).to_string()
}

/// Generate a canonical URI string from the given URL.
pub fn canonical_uri_string(uri: &Url) -> String {
    // decode `Url`'s percent-encoding and then reencode it
    // according to AWS's rules
    let decoded = percent_encoding::percent_decode_str(uri.path()).decode_utf8_lossy();
    uri_encode(&decoded, false)
}

/// Generate a canonical query string from the query pairs in the given URL.
pub fn canonical_query_string(uri: &Url) -> String {
    let mut keyvalues = uri
        .query_pairs()
        .map(|(key, value)| uri_encode(&key, true) + "=" + &uri_encode(&value, true))
        .collect::<Vec<String>>();
    keyvalues.sort();
    keyvalues.join("&")
}

/// Generate a canonical header string from the provided headers.
pub fn canonical_header_string(headers: &HeaderMap) -> String {
    let mut keyvalues = headers
        .iter()
        .map(|(key, value)| {
            // Values that are not strings are silently dropped (AWS wouldn't
            // accept them anyway)
            key.as_str().to_lowercase() + ":" + value.to_str().unwrap().trim()
        })
        .collect::<Vec<String>>();
    keyvalues.sort();
    keyvalues.join("\n")
}

/// Generate a signed header string from the provided headers.
pub fn signed_header_string(headers: &HeaderMap) -> String {
    let mut keys = headers
        .keys()
        .map(|key| key.as_str().to_lowercase())
        .collect::<Vec<String>>();
    keys.sort();
    keys.join(";")
}

/// Generate a canonical request.
pub fn canonical_request(method: &str, url: &Url, headers: &HeaderMap, sha256: &str) -> String {
    format!(
        "{method}\n{uri}\n{query_string}\n{headers}\n\n{signed}\n{sha256}",
        method = method,
        uri = canonical_uri_string(url),
        query_string = canonical_query_string(url),
        headers = canonical_header_string(headers),
        signed = signed_header_string(headers),
        sha256 = sha256
    )
}

/// Generate an AWS scope string.
pub fn scope_string(datetime: &DateTime<Utc>, region: &Region) -> String {
    format!(
        "{date}/{region}/s3/aws4_request",
        date = datetime.format(SHORT_DATE),
        region = region
    )
}

/// Generate the "string to sign" - the value to which the HMAC signing is
/// applied to sign requests.
pub fn string_to_sign(datetime: &DateTime<Utc>, region: &Region, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req.as_bytes());
    let string_to = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hash}",
        timestamp = datetime.format(LONG_DATETIME),
        scope = scope_string(datetime, region),
        hash = hex::encode(hasher.finalize().as_slice())
    );
    string_to
}

/// Generate the AWS signing key, derived from the secret key, date, region,
/// and service name.
pub fn signing_key(
    datetime: &DateTime<Utc>,
    secret_key: &str,
    region: &Region,
    service: &str,
) -> Result<Vec<u8>> {
    let secret = format!("AWS4{}", secret_key);
    let mut date_hmac = HmacSha256::new_varkey(secret.as_bytes()).map_err(|e| anyhow! {"{}",e})?;
    date_hmac.update(datetime.format(SHORT_DATE).to_string().as_bytes());
    let mut region_hmac =
        HmacSha256::new_varkey(&date_hmac.finalize().into_bytes()).map_err(|e| anyhow! {"{}",e})?;
    region_hmac.update(region.to_string().as_bytes());
    let mut service_hmac = HmacSha256::new_varkey(&region_hmac.finalize().into_bytes())
        .map_err(|e| anyhow! {"{}",e})?;
    service_hmac.update(service.as_bytes());
    let mut signing_hmac = HmacSha256::new_varkey(&service_hmac.finalize().into_bytes())
        .map_err(|e| anyhow! {"{}",e})?;
    signing_hmac.update(b"aws4_request");
    Ok(signing_hmac.finalize().into_bytes().to_vec())
}

/// Generate the AWS authorization header.
pub fn authorization_header(
    access_key: &str,
    datetime: &DateTime<Utc>,
    region: &Region,
    signed_headers: &str,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope},\
            SignedHeaders={signed_headers},Signature={signature}",
        access_key = access_key,
        scope = scope_string(datetime, region),
        signed_headers = signed_headers,
        signature = signature
    )
}

pub fn authorization_query_params_no_sig(
    access_key: &str,
    datetime: &DateTime<Utc>,
    region: &Region,
    expires: u32,
    custom_headers: Option<&HeaderMap>,
    token: Option<&str>,
) -> Result<String> {
    let credentials = uri_encode(
        &format!("{}/{}", access_key, scope_string(datetime, region)),
        true,
    );

    let mut signed_headers = vec!["host".to_string()];

    if let Some(custom_headers) = &custom_headers {
        for k in custom_headers.keys() {
            signed_headers.push(k.to_string())
        }
    }

    let signed_headers_string = uri_encode(&signed_headers.join(";"), true);

    let mut query_params = format!(
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256\
            &X-Amz-Credential={credentials}\
            &X-Amz-Date={long_date}\
            &X-Amz-Expires={expires}\
            &X-Amz-SignedHeaders={signed_headers}",
        credentials = credentials,
        long_date = datetime.format(LONG_DATETIME).to_string(),
        expires = expires,
        signed_headers = signed_headers_string
    );

    if let Some(token) = token {
        query_params.push_str(&format!(
            "&X-Amz-Security-Token={}",
            uri_encode(token, true)
        ))
    }

    Ok(query_params)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use std::str;
    use url::Url;

    use super::*;

    use crate::serde_types::ListBucketResult;
    use http::header::{HeaderName, HOST, RANGE};
    use http::HeaderMap;
    use serde_xml_rs as serde_xml;

    #[test]
    fn test_base_url_encode() {
        // Make sure parsing doesn't remove extra slashes, as normalization
        // will mess up the path lookup.
        let url = Url::parse("http://s3.amazonaws.com/examplebucket///foo//bar//baz").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!("/examplebucket///foo//bar//baz", canonical);
    }

    #[test]
    fn test_path_encode() {
        let url = Url::parse("http://s3.amazonaws.com/bucket/Filename (xx)%=").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!("/bucket/Filename%20%28xx%29%25%3D", canonical);
    }

    #[test]
    fn test_path_slash_encode() {
        let url =
            Url::parse("http://s3.amazonaws.com/bucket/Folder (xx)%=/Filename (xx)%=").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!(
            "/bucket/Folder%20%28xx%29%25%3D/Filename%20%28xx%29%25%3D",
            canonical
        );
    }

    #[test]
    fn test_query_string_encode() {
        let url = Url::parse(
            "http://s3.amazonaws.com/examplebucket?\
                              prefix=somePrefix&marker=someMarker&max-keys=20",
        )
        .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("marker=someMarker&max-keys=20&prefix=somePrefix", canonical);

        let url = Url::parse("http://s3.amazonaws.com/examplebucket?acl").unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("acl=", canonical);

        let url = Url::parse(
            "http://s3.amazonaws.com/examplebucket?\
                              key=with%20space&also+space=with+plus",
        )
        .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("also%20space=with%20plus&key=with%20space", canonical);
    }

    #[test]
    fn test_headers_encode() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            "20130708T220855Z".parse().unwrap(),
        );
        headers.insert(HeaderName::from_static("foo"), "bAr".parse().unwrap());
        headers.insert(HOST, "s3.amazonaws.com".parse().unwrap());
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
        let signature = signing_key(&datetime, key, &"us-east-1".parse().unwrap(), "iam").unwrap();
        assert_eq!(expected, hex::encode(signature));
    }

    const EXPECTED_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb924\
                                        27ae41e4649b934ca495991b7852b855";

    #[rustfmt::skip]
    const EXPECTED_CANONICAL_REQUEST: &str =
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

    #[rustfmt::skip]
    const EXPECTED_STRING_TO_SIGN: &str =
        "AWS4-HMAC-SHA256\n\
         20130524T000000Z\n\
         20130524/us-east-1/s3/aws4_request\n\
         7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";

    #[test]
    fn test_signing() {
        let url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            "20130524T000000Z".parse().unwrap(),
        );
        headers.insert(RANGE, "bytes=0-9".parse().unwrap());
        headers.insert(HOST, "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            EXPECTED_SHA.parse().unwrap(),
        );
        let canonical = canonical_request("GET", &url, &headers, EXPECTED_SHA);
        assert_eq!(EXPECTED_CANONICAL_REQUEST, canonical);

        let datetime = Utc.ymd(2013, 5, 24).and_hms(0, 0, 0);
        let string_to_sign = string_to_sign(&datetime, &"us-east-1".parse().unwrap(), &canonical);
        assert_eq!(EXPECTED_STRING_TO_SIGN, string_to_sign);

        let expected = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let signing_key = signing_key(&datetime, secret, &"us-east-1".parse().unwrap(), "s3");
        let mut hmac = Hmac::<Sha256>::new_varkey(&signing_key.unwrap()).unwrap();
        hmac.update(string_to_sign.as_bytes());
        assert_eq!(expected, hex::encode(hmac.finalize().into_bytes()));
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
        let deserialized: ListBucketResult =
            serde_xml::from_reader(result_string.as_bytes()).expect("Parse error!");
        assert!(deserialized.is_truncated);
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode(r#"~!@#$%^&*()-_=+[]\{}|;:'",.<>? привет 你好"#, true), "~%21%40%23%24%25%5E%26%2A%28%29-_%3D%2B%5B%5D%5C%7B%7D%7C%3B%3A%27%22%2C.%3C%3E%3F%20%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82%20%E4%BD%A0%E5%A5%BD");
    }
}
