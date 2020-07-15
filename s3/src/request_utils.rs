use crate::bucket::Bucket;
use crate::command::Command;
use crate::signing;
use crate::Result;
use chrono::{DateTime, Utc};
use hmac::Mac;
use std::collections::HashMap;
use url::Url;
use sha2::{Digest, Sha256};
use crate::EMPTY_PAYLOAD_SHA;
use crate::LONG_DATE;

pub trait Request {
    fn command(&self) -> &Command<'_>;
    fn datetime(&self) -> DateTime<Utc>;
    fn bucket(&self) -> &Bucket;
    fn path(&self) -> &str;
}

pub fn presigned(r: &impl Request) -> Result<String> {
    let expiry = match r.command() {
        Command::PresignGet { expiry_secs } => expiry_secs,
        Command::PresignPut { expiry_secs } => expiry_secs,
        _ => unreachable!(),
    };
    let authorization = presigned_authorization(r)?;
    Ok(format!(
        "{}&X-Amz-Signature={}",
        presigned_url_no_sig(r, *expiry)?,
        authorization
    ))
}

fn presigned_authorization(r: &impl Request) -> Result<String> {
    let mut headers: HashMap<String, String> = HashMap::new();
    let host_header = host_header(r);
    headers.insert("HOST".to_string(), host_header);
    let canonical_request = presigned_canonical_request(r, &headers)?;
    let string_to_sign = string_to_sign(r, &canonical_request);
    let mut hmac = signing::HmacSha256::new_varkey(&signing_key(r)?)?;
    hmac.input(string_to_sign.as_bytes());
    let signature = hex::encode(hmac.result().code());
    // let signed_header = signing::signed_header_string(&headers);
    Ok(signature)
}

fn presigned_canonical_request(
    r: &impl Request,
    headers: &HashMap<String, String>,
) -> Result<String> {
    let expiry = match r.command() {
        Command::PresignGet { expiry_secs } => expiry_secs,
        Command::PresignPut { expiry_secs } => expiry_secs,
        _ => unreachable!(),
    };
    let canonical_request = signing::canonical_request(
        &r.command().http_verb().to_string(),
        &presigned_url_no_sig(r, *expiry)?,
        headers,
        "UNSIGNED-PAYLOAD",
    );
    Ok(canonical_request)
}

fn presigned_url_no_sig(r: &impl Request, expiry: u32) -> Result<Url> {
    Ok(Url::parse(&format!(
        "{}{}",
        url(r),
        signing::authorization_query_params_no_sig(
            &r.bucket().access_key().unwrap(),
            &r.datetime(),
            &r.bucket().region(),
            expiry
        )
    ))?)
}

pub fn authorization(r: &impl Request, headers: &HashMap<String, String>) -> Result<String> {
    let canonical_request = canonical_request(r, headers);
    let string_to_sign = string_to_sign(r, &canonical_request);
    let mut hmac = signing::HmacSha256::new_varkey(&signing_key(r)?)?;
    hmac.input(string_to_sign.as_bytes());
    let signature = hex::encode(hmac.result().code());
    let signed_header = signing::signed_header_string(headers);
    Ok(signing::authorization_header(
        &r.bucket().access_key().unwrap(),
        &r.datetime(),
        &r.bucket().region(),
        &signed_header,
        &signature,
    ))
}

fn canonical_request(r: &impl Request, headers: &HashMap<String, String>) -> String {
    signing::canonical_request(
        &r.command().http_verb().to_string(),
        &url(r),
        headers,
        &sha256(r),
    )
}

pub fn sha256(r: &impl Request) -> String {
    match r.command() {
        Command::PutObject { content, .. } => {
            let mut sha = Sha256::default();
            sha.input(content);
            hex::encode(sha.result().as_slice())
        }
        Command::PutObjectTagging { tags } => {
            let mut sha = Sha256::default();
            sha.input(tags.as_bytes());
            hex::encode(sha.result().as_slice())
        }
        _ => EMPTY_PAYLOAD_SHA.into(),
    }
}

pub fn host_header(r: &impl Request) -> String {
    r.bucket().host()
}

fn string_to_sign(r: &impl Request, request: &str) -> String {
    signing::string_to_sign(&r.datetime(), &r.bucket().region(), request)
}

fn signing_key(r: &impl Request) -> Result<Vec<u8>> {
    Ok(signing::signing_key(
        &r.datetime(),
        &r.bucket()
            .secret_key()
            .expect("Secret key must be provided to sign headers, found None"),
        &r.bucket().region(),
        "s3",
    )?)
}

pub fn url(r: &impl Request) -> Url {
    let mut url_str = r.bucket().url();

    if !r.path().starts_with('/') {
        url_str.push_str("/");
    }

    url_str.push_str(r.path());

    // Since every part of this URL is either pre-encoded or statically
    // generated, there's really no way this should fail.
    let mut url = Url::parse(&url_str).expect("static URL parsing");

    for (key, value) in &r.bucket().extra_query {
        url.query_pairs_mut().append_pair(key, value);
    }

    if let Command::ListBucket {
        prefix,
        delimiter,
        continuation_token,
        start_after,
        max_keys,
    } = r.command().clone()
    {
        let mut query_pairs = url.query_pairs_mut();
        delimiter.map(|d| query_pairs.append_pair("delimiter", &d));
        query_pairs.append_pair("prefix", &prefix);
        query_pairs.append_pair("list-type", "2");
        if let Some(token) = continuation_token {
            query_pairs.append_pair("continuation-token", &token);
        }
        if let Some(start_after) = start_after {
            query_pairs.append_pair("start-after", &start_after);
        }
        if let Some(max_keys) = max_keys {
            query_pairs.append_pair("max-keys", &max_keys.to_string());
        }
    }

    match r.command() {
        Command::PutObjectTagging { .. }
        | Command::GetObjectTagging
        | Command::DeleteObjectTagging => {
            url.query_pairs_mut().append_pair("tagging", "");
        }
        _ => {}
    }

    url
}

pub fn content_length(r: &impl Request) -> usize {
    match r.command() {
        Command::PutObject { content, .. } => content.len(),
        Command::PutObjectTagging { tags } => tags.len(),
        _ => 0,
    }
}

pub fn content_type(r: &impl Request) -> String {
    match r.command() {
        Command::PutObject { content_type, .. } => content_type.to_string(),
        _ => "text/plain".into(),
    }
}

pub fn long_date(r: &impl Request) -> String {
    r.datetime().format(LONG_DATE).to_string()
}