use async_std::fs::File;
use async_std::path::Path;
use crate::bucket::CHUNK_SIZE;
use futures::io::{AsyncRead, AsyncReadExt};
use crate::Result;


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
            break
        }
    }
    let digest = format!("{:x}", md5::compute(digests));
    let etag = format!("{}-{}", digest, chunks);
    Ok(etag)
}

pub async fn read_chunk<R: AsyncRead + Unpin>(reader: &mut R,) -> Result<Vec<u8>> {
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    loop {
        let mut buffer = [0; 5000];
        let mut take = reader.take(5000);
        let n = take.read(&mut buffer).await?;
        if n < 5000 {
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

        assert_eq!(etag, "e0eea6e137ee28451311511bfa58cdea-2");
    }
    
}