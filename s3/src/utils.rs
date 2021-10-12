use std::str::FromStr;

use crate::{bucket::CHUNK_SIZE, serde_types::HeadObjectResult};
use anyhow::Result;

#[cfg(feature = "with-async-std")]
use async_std::fs::File;
#[cfg(feature = "sync")]
use std::fs::File;
#[cfg(feature = "with-tokio")]
use tokio::fs::File;

#[cfg(feature = "with-async-std")]
use async_std::path::Path;
#[cfg(any(feature = "sync", feature = "with-tokio"))]
use std::path::Path;

#[cfg(feature = "with-async-std")]
use futures_io::{AsyncRead, AsyncReadExt};
#[cfg(feature = "sync")]
use std::io::Read;
#[cfg(feature = "with-tokio")]
use tokio::io::{AsyncRead, AsyncReadExt};

/// # Example
/// ```rust,no_run
/// use s3::utils::etag_for_path;
///
/// #[tokio::main]
/// async fn main() {
///     let path = "test_etag";
///     let etag = etag_for_path(path).await.unwrap();
///     println!("{}", etag);
/// }
/// ```
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub async fn etag_for_path(path: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(path).await?;
    let mut digests = Vec::new();
    let mut chunks = 0;
    loop {
        let chunk = read_chunk(&mut file).await?;
        let digest: [u8; 16] = md5::compute(&chunk).into();
        digests.extend_from_slice(&digest);
        chunks += 1;
        if chunk.len() < CHUNK_SIZE {
            break;
        }
    }
    let digest = format!("{:x}", md5::compute(digests));
    let etag = if chunks <= 1 {
        digest
    } else {
        format!("{}-{}", digest, chunks)
    };
    Ok(etag)
}

/// # Example
/// ```rust,no_run
/// use s3::utils::etag_for_path;
///
/// let path = "test_etag";
/// let etag = etag_for_path(path).unwrap();
/// println!("{}", etag);
/// ```
#[cfg(feature = "sync")]
pub fn etag_for_path(path: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digests = Vec::new();
    let mut chunks = 0;
    loop {
        let chunk = read_chunk(&mut file)?;
        let digest: [u8; 16] = md5::compute(&chunk).into();
        digests.extend_from_slice(&digest);
        chunks += 1;
        if chunk.len() < CHUNK_SIZE {
            break;
        }
    }
    let digest = format!("{:x}", md5::compute(digests));
    let etag = if chunks <= 1 {
        digest
    } else {
        format!("{}-{}", digest, chunks)
    };
    Ok(etag)
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub async fn read_chunk<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    let mut take = reader.take(CHUNK_SIZE as u64);
    take.read_to_end(&mut chunk).await?;

    Ok(chunk)
}

#[cfg(feature = "sync")]
pub fn read_chunk<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    let mut take = reader.take(CHUNK_SIZE as u64);
    take.read_to_end(&mut chunk)?;

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

impl From<&http::HeaderMap> for HeadObjectResult {
    fn from(headers: &http::HeaderMap) -> Self {
        let mut result = HeadObjectResult {
            accept_ranges: headers.get_string("accept-ranges"),
            cache_control: headers.get_string("Cache-Control"),
            content_disposition: headers.get_string("Content-Disposition"),
            content_encoding: headers.get_string("Content-Encoding"),
            content_language: headers.get_string("Content-Language"),
            content_length: headers.get_and_convert("Content-Length"),
            content_type: headers.get_string("Content-Type"),
            delete_marker: headers.get_and_convert("x-amz-delete-marker"),
            e_tag: headers.get_string("ETag"),
            expiration: headers.get_string("x-amz-expiration"),
            expires: headers.get_string("Expires"),
            last_modified: headers.get_string("Last-Modified"),
            ..Default::default()
        };
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

#[cfg(test)]
mod test {
    use crate::utils::etag_for_path;
    #[cfg(feature = "with-async-std")]
    use async_std::io::Cursor;
    use std::fs::File;
    use std::io::prelude::*;
    #[cfg(any(feature = "with-tokio", feature = "sync"))]
    use std::io::Cursor;

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_etag() {
        let path = "test_etag";
        std::fs::remove_file(path).unwrap_or_else(|_| {});
        let test: Vec<u8> = object(10_000_000);

        let mut file = File::create(path).unwrap();
        file.write_all(&test).unwrap();

        let etag = etag_for_path(path).await.unwrap();

        std::fs::remove_file(path).unwrap_or_else(|_| {});

        assert_eq!(etag, "e438487f09f09c042b2de097765e5ac2-2");
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_read_chunk_all_zero() {
        let blob = vec![0u8; 10_000_000];
        let mut blob = Cursor::new(blob);

        let result = super::read_chunk(&mut blob).await.unwrap();

        assert_eq!(result.len(), crate::bucket::CHUNK_SIZE);
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn test_read_chunk_multi_chunk() {
        let blob = vec![1u8; 10_000_000];
        let mut blob = Cursor::new(blob);

        let result = super::read_chunk(&mut blob).await.unwrap();
        assert_eq!(result.len(), crate::bucket::CHUNK_SIZE);

        let result = super::read_chunk(&mut blob).await.unwrap();
        assert_eq!(result.len(), 1_611_392);
    }
}
