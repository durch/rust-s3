use crate::{Bucket, Region};
use anyhow::Result;

/// [AWS Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL)
#[allow(dead_code)]
#[derive(Clone, Debug)]
enum CannedBucketAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
}

use http::header::HeaderName;
use http::HeaderMap;
use std::fmt;

impl fmt::Display for CannedBucketAcl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CannedBucketAcl::Private => write!(f, "private"),
            CannedBucketAcl::PublicRead => write!(f, "public-read"),
            CannedBucketAcl::PublicReadWrite => write!(f, "public-read-write"),
            CannedBucketAcl::AuthenticatedRead => write!(f, "authenticated-read"),
        }
    }
}

/// [AWS Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html)
#[allow(dead_code)]
#[derive(Clone, Debug)]
enum BucketAcl {
    Id { id: String },
    Uri { uri: String },
    Email { email: String },
}

impl fmt::Display for BucketAcl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketAcl::Id { id } => write!(f, "id=\"{}\"", id),
            BucketAcl::Uri { uri } => write!(f, "uri=\"{}\"", uri),
            BucketAcl::Email { email } => write!(f, "email=\"{}\"", email),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BucketConfiguration {
    acl: CannedBucketAcl,
    object_lock_enabled: bool,
    grant_full_control: Option<Vec<BucketAcl>>,
    grant_read: Option<Vec<BucketAcl>>,
    grant_read_acp: Option<Vec<BucketAcl>>,
    grant_write: Option<Vec<BucketAcl>>,
    grant_write_acp: Option<Vec<BucketAcl>>,
    location_constraint: Option<Region>,
}

impl Default for BucketConfiguration {
    fn default() -> Self {
        BucketConfiguration::private()
    }
}

fn acl_list(acl: &[BucketAcl]) -> String {
    acl.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

impl BucketConfiguration {
    pub fn public() -> Self {
        BucketConfiguration {
            acl: CannedBucketAcl::PublicReadWrite,
            object_lock_enabled: false,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write: None,
            grant_write_acp: None,
            location_constraint: None,
        }
    }

    pub fn private() -> Self {
        BucketConfiguration {
            acl: CannedBucketAcl::Private,
            object_lock_enabled: false,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write: None,
            grant_write_acp: None,
            location_constraint: None,
        }
    }

    pub fn set_region(&mut self, region: Region) {
        self.set_location_constraint(region)
    }

    pub fn set_location_constraint(&mut self, region: Region) {
        self.location_constraint = Some(region)
    }

    pub fn location_constraint_payload(&self) -> Option<String> {
        if let Some(ref location_constraint) = self.location_constraint {
            if location_constraint == &Region::UsEast1 {
                return None;
            }
            Some(format!(
                "<CreateBucketConfiguration><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                location_constraint.to_string()
            ))
        } else {
            None
        }
    }

    pub fn add_headers(&self, headers: &mut HeaderMap) -> Result<()> {
        headers.insert(
            HeaderName::from_static("x-amz-acl"),
            self.acl.to_string().parse().unwrap(),
        );
        if self.object_lock_enabled {
            headers.insert(
                HeaderName::from_static("x-amz-bucket-object-lock-enabled"),
                "Enabled".to_string().parse().unwrap(),
            );
        }
        if let Some(ref value) = self.grant_full_control {
            headers.insert(
                HeaderName::from_static("x-amz-grant-full-control"),
                acl_list(value).parse().unwrap(),
            );
        }
        if let Some(ref value) = self.grant_read {
            headers.insert(
                HeaderName::from_static("x-amz-grant-read"),
                acl_list(value).parse().unwrap(),
            );
        }
        if let Some(ref value) = self.grant_read_acp {
            headers.insert(
                HeaderName::from_static("x-amz-grant-read-acp"),
                acl_list(value).parse().unwrap(),
            );
        }
        if let Some(ref value) = self.grant_write {
            headers.insert(
                HeaderName::from_static("x-amz-grant-write"),
                acl_list(value).parse().unwrap(),
            );
        }
        if let Some(ref value) = self.grant_write_acp {
            headers.insert(
                HeaderName::from_static("x-amz-grant-write-acp"),
                acl_list(value).parse().unwrap(),
            );
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub struct CreateBucketResponse {
    pub bucket: Bucket,
    pub response_text: String,
    pub response_code: u16,
}

impl CreateBucketResponse {
    pub fn success(&self) -> bool {
        self.response_code == 200
    }
}
