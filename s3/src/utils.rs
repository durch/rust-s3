use crate::bucket::CHUNK_SIZE;
use crate::Result;
use async_std::fs::File;
use async_std::path::Path;
use futures::io::{AsyncRead, AsyncReadExt};
use std::io::Read;

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

pub fn read_chunk_blocking<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
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

#[cfg(test)]
mod test {
    use crate::utils::etag_for_path;
    use std::fs::File;
    use std::io::prelude::*;

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    #[tokio::test]
    async fn test_etag() {
        let path = "test_etag";
        std::fs::remove_file(path).unwrap_or_else(|_| {});
        let test: Vec<u8> = object(10_000_000);

        let mut file = File::create(path).unwrap();
        file.write_all(&test).unwrap();

        let etag = etag_for_path(path).await.unwrap();

        std::fs::remove_file(path).unwrap_or_else(|_| {});

        assert_eq!(etag, "ae890066cc055c740b3dc3c8854a643b-2");
    }
}
