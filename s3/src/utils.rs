use std::str::FromStr;

use crate::Result;
use crate::{bucket::CHUNK_SIZE, serde_types::HeadObjectResult};
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use async_std::fs::File;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use async_std::path::Path;
#[cfg(feature = "with-async-std")]
use futures::io::{AsyncRead, AsyncReadExt};
#[cfg(feature = "with-tokio")]
use tokio::io::{AsyncRead, AsyncReadExt};
use std::collections::HashMap;
#[cfg(feature = "sync")]
use std::fs::File;
#[cfg(feature = "sync")]
use std::io::Read;
#[cfg(feature = "sync")]
use std::path::Path;

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub async fn read_chunk<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    const LOCAL_CHUNK_SIZE: usize = 8388;
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    loop {
        let mut buffer = [0; LOCAL_CHUNK_SIZE];
        let mut take = reader.take(LOCAL_CHUNK_SIZE as u64);
        let n = take.read(&mut buffer).await?;
        if n < LOCAL_CHUNK_SIZE {
            buffer.reverse();
            let mut trim_buffer = buffer
                .iter()
                .skip_while(|x| **x == 0)
                .copied()
                .collect::<Vec<u8>>();
            trim_buffer.reverse();
            chunk.extend_from_slice(&trim_buffer);
            chunk.shrink_to_fit();
            break;
        } else {
            chunk.extend_from_slice(&buffer);
            if chunk.len() >= CHUNK_SIZE {
                break;
            } else {
                continue;
            }
        }
    }
    Ok(chunk)
}

#[cfg(feature = "sync")]
pub fn read_chunk<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    const LOCAL_CHUNK_SIZE: usize = 8388;
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    loop {
        let mut buffer = [0; LOCAL_CHUNK_SIZE];
        let mut take = reader.take(LOCAL_CHUNK_SIZE as u64);
        let n = take.read(&mut buffer)?;
        if n < LOCAL_CHUNK_SIZE {
            buffer.reverse();
            let mut trim_buffer = buffer
                .iter()
                .skip_while(|x| **x == 0)
                .copied()
                .collect::<Vec<u8>>();
            trim_buffer.reverse();
            chunk.extend_from_slice(&trim_buffer);
            chunk.shrink_to_fit();
            break;
        } else {
            chunk.extend_from_slice(&buffer);
            if chunk.len() >= CHUNK_SIZE {
                break;
            } else {
                continue;
            }
        }
    }
    Ok(chunk)
}
pub trait GetAndConvertHeaders {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T>;
    fn get_string(&self, header: &str) -> Option<String>;
}

impl GetAndConvertHeaders for http::header::HeaderMap {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T> {
        self.get(header)?.to_str().ok()?.parse::<T>().ok()
    }
    fn get_string(&self, header: &str) -> Option<String> {
        Some(self.get(header)?.to_str().ok()?.to_owned())
    }
}

impl GetAndConvertHeaders for HashMap<String, String> {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T> {
        if let Some(header) = self.get(header) {
            header.clone().parse::<T>().ok()
        } else {
            None
        }
    }
    fn get_string(&self, header: &str) -> Option<String> {
        self.get(header).cloned()
    }
}

impl From<&http::HeaderMap> for HeadObjectResult {
    fn from(headers: &http::HeaderMap) -> Self {
        let mut result = HeadObjectResult::default();
        result.accept_ranges = headers.get_string("accept-ranges");
        result.cache_control = headers.get_string("Cache-Control");
        result.content_disposition = headers.get_string("Content-Disposition");
        result.content_encoding = headers.get_string("Content-Encoding");
        result.content_language = headers.get_string("Content-Language");
        result.content_length = headers.get_and_convert("Content-Length");
        result.content_type = headers.get_string("Content-Type");
        result.delete_marker = headers.get_and_convert("x-amz-delete-marker");
        result.e_tag = headers.get_string("ETag");
        result.expiration = headers.get_string("x-amz-expiration");
        result.expires = headers.get_string("Expires");
        result.last_modified = headers.get_string("Last-Modified");
        let mut values = ::std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if key.as_str().starts_with("x-amz-meta-") {
                if let Ok(value) = value.to_str() {
                    values.insert(
                        key.as_str()["x-amz-meta-".len()..].to_owned(),
                        value.to_owned(),
                    );
                }
            }
        }
        result.metadata = Some(values);
        result.missing_meta = headers.get_and_convert("x-amz-missing-meta");
        result.object_lock_legal_hold_status = headers.get_string("x-amz-object-lock-legal-hold");
        result.object_lock_mode = headers.get_string("x-amz-object-lock-mode");
        result.object_lock_retain_until_date =
            headers.get_string("x-amz-object-lock-retain-until-date");
        result.parts_count = headers.get_and_convert("x-amz-mp-parts-count");
        result.replication_status = headers.get_string("x-amz-replication-status");
        result.request_charged = headers.get_string("x-amz-request-charged");
        result.restore = headers.get_string("x-amz-restore");
        result.sse_customer_algorithm =
            headers.get_string("x-amz-server-side-encryption-customer-algorithm");
        result.sse_customer_key_md5 =
            headers.get_string("x-amz-server-side-encryption-customer-key-MD5");
        result.ssekms_key_id = headers.get_string("x-amz-server-side-encryption-aws-kms-key-id");
        result.server_side_encryption = headers.get_string("x-amz-server-side-encryption");
        result.storage_class = headers.get_string("x-amz-storage-class");
        result.version_id = headers.get_string("x-amz-version-id");
        result.website_redirect_location = headers.get_string("x-amz-website-redirect-location");
        result
    }
}
// Assumes all keys are lowercase
impl From<&HashMap<String, String>> for HeadObjectResult {
    fn from(headers: &HashMap<String, String>) -> Self {
        let mut result = HeadObjectResult::default();
        result.accept_ranges = headers.get_string("accept-ranges");
        result.cache_control = headers.get_string("cache-control");
        result.content_disposition = headers.get_string("content-cisposition");
        result.content_encoding = headers.get_string("content-encoding");
        result.content_language = headers.get_string("content-language");
        result.content_length = headers.get_and_convert("content-length");
        result.content_type = headers.get_string("content-type");
        result.delete_marker = headers.get_and_convert("x-amz-delete-marker");
        result.e_tag = headers.get_string("etag");
        result.expiration = headers.get_string("x-amz-expiration");
        result.expires = headers.get_string("expires");
        result.last_modified = headers.get_string("last-modified");
        let mut values = ::std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if key.as_str().starts_with("x-amz-meta-") {
                values.insert(
                    key.as_str()["x-amz-meta-".len()..].to_owned(),
                    value.to_owned(),
                );
            }
        }
        result.metadata = Some(values);
        result.missing_meta = headers.get_and_convert("x-amz-missing-meta");
        result.object_lock_legal_hold_status = headers.get_string("x-amz-object-lock-legal-hold");
        result.object_lock_mode = headers.get_string("x-amz-object-lock-mode");
        result.object_lock_retain_until_date =
            headers.get_string("x-amz-object-lock-retain-until-date");
        result.parts_count = headers.get_and_convert("x-amz-mp-parts-count");
        result.replication_status = headers.get_string("x-amz-replication-status");
        result.request_charged = headers.get_string("x-amz-request-charged");
        result.restore = headers.get_string("x-amz-restore");
        result.sse_customer_algorithm =
            headers.get_string("x-amz-server-side-encryption-customer-algorithm");
        result.sse_customer_key_md5 =
            headers.get_string("x-amz-server-side-encryption-customer-key-md5");
        result.ssekms_key_id = headers.get_string("x-amz-server-side-encryption-aws-kms-key-id");
        result.server_side_encryption = headers.get_string("x-amz-server-side-encryption");
        result.storage_class = headers.get_string("x-amz-storage-class");
        result.version_id = headers.get_string("x-amz-version-id");
        result.website_redirect_location = headers.get_string("x-amz-website-redirect-location");
        result
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::prelude::*;

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(all(not(feature = "sync"), feature = "with-async-std"), tokio::test)
    )]
    async fn test_etag() {
        let path = "test_etag";
        std::fs::remove_file(path).unwrap_or_else(|_| {});
        let test: Vec<u8> = object(10_000_000);

        let mut file = File::create(path).unwrap();
        file.write_all(&test).unwrap();

        std::fs::remove_file(path).unwrap_or_else(|_| {});

    }
}
