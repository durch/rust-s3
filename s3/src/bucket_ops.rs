use crate::error::S3Error;
use crate::{Bucket, Region};

/// [AWS Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL)
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum CannedBucketAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
    Custom(String),
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
            CannedBucketAcl::Custom(policy) => write!(f, "{policy}"),
        }
    }
}

/// [AWS Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html)
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum BucketAcl {
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
    acl: Option<CannedBucketAcl>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        acl: Option<CannedBucketAcl>,
        object_lock_enabled: bool,
        grant_full_control: Option<Vec<BucketAcl>>,
        grant_read: Option<Vec<BucketAcl>>,
        grant_read_acp: Option<Vec<BucketAcl>>,
        grant_write: Option<Vec<BucketAcl>>,
        grant_write_acp: Option<Vec<BucketAcl>>,
        location_constraint: Option<Region>,
    ) -> Self {
        Self {
            acl,
            object_lock_enabled,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write,
            grant_write_acp,
            location_constraint,
        }
    }

    pub fn public() -> Self {
        BucketConfiguration {
            acl: None,
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
            acl: Some(CannedBucketAcl::Private),
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
                location_constraint
            ))
        } else {
            None
        }
    }

    pub fn add_headers(&self, headers: &mut HeaderMap) -> Result<(), S3Error> {
        if let Some(ref acl) = self.acl {
            headers.insert(
                HeaderName::from_static("x-amz-acl"),
                acl.to_string().parse()?,
            );
        }

        if self.object_lock_enabled {
            headers.insert(
                HeaderName::from_static("x-amz-bucket-object-lock-enabled"),
                "Enabled".to_string().parse()?,
            );
        }
        if let Some(ref value) = self.grant_full_control {
            headers.insert(
                HeaderName::from_static("x-amz-grant-full-control"),
                acl_list(value).parse()?,
            );
        }
        if let Some(ref value) = self.grant_read {
            headers.insert(
                HeaderName::from_static("x-amz-grant-read"),
                acl_list(value).parse()?,
            );
        }
        if let Some(ref value) = self.grant_read_acp {
            headers.insert(
                HeaderName::from_static("x-amz-grant-read-acp"),
                acl_list(value).parse()?,
            );
        }
        if let Some(ref value) = self.grant_write {
            headers.insert(
                HeaderName::from_static("x-amz-grant-write"),
                acl_list(value).parse()?,
            );
        }
        if let Some(ref value) = self.grant_write_acp {
            headers.insert(
                HeaderName::from_static("x-amz-grant-write-acp"),
                acl_list(value).parse()?,
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

pub use list_buckets::*;

mod list_buckets {

    #[derive(Clone, Default, Deserialize, Debug)]
    #[serde(rename_all = "PascalCase", rename = "ListAllMyBucketsResult")]
    pub struct ListBucketsResponse {
        pub owner: BucketOwner,
        pub buckets: BucketContainer,
    }

    impl ListBucketsResponse {
        pub fn bucket_names(&self) -> impl Iterator<Item = String> + '_ {
            self.buckets.bucket.iter().map(|bucket| bucket.name.clone())
        }
    }

    #[derive(Deserialize, Default, Clone, Debug, PartialEq, Eq)]
    pub struct BucketOwner {
        #[serde(rename = "ID")]
        pub id: String,
        #[serde(rename = "DisplayName")]
        pub display_name: Option<String>,
    }

    #[derive(Deserialize, Default, Clone, Debug)]
    #[serde(rename_all = "PascalCase")]
    pub struct BucketInfo {
        pub name: String,
        pub creation_date: String,
    }

    #[derive(Deserialize, Default, Clone, Debug)]
    #[serde(rename_all = "PascalCase")]
    pub struct BucketContainer {
        #[serde(default)]
        pub bucket: Vec<BucketInfo>,
    }

    #[cfg(test)]
    mod tests {
        #[test]
        pub fn parse_list_buckets_response() {
            let response = r#"
            <?xml version="1.0" encoding="UTF-8"?>
                <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Owner>
                        <ID>02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4</ID>
                        <DisplayName>minio</DisplayName>
                    </Owner>
                    <Buckets>
                        <Bucket>
                            <Name>test-rust-s3</Name>
                            <CreationDate>2023-06-04T20:13:37.837Z</CreationDate>
                        </Bucket>
                        <Bucket>
                            <Name>test-rust-s3-2</Name>
                            <CreationDate>2023-06-04T20:17:47.152Z</CreationDate>
                        </Bucket>
                    </Buckets>
                </ListAllMyBucketsResult>
            "#;

            let parsed = quick_xml::de::from_str::<super::ListBucketsResponse>(response).unwrap();

            assert_eq!(parsed.owner.display_name, "minio");
            assert_eq!(
                parsed.owner.id,
                "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
            );
            assert_eq!(parsed.buckets.bucket.len(), 2);

            assert_eq!(parsed.buckets.bucket.first().unwrap().name, "test-rust-s3");
            assert_eq!(
                parsed.buckets.bucket.first().unwrap().creation_date,
                "2023-06-04T20:13:37.837Z"
            );

            assert_eq!(parsed.buckets.bucket.last().unwrap().name, "test-rust-s3-2");
            assert_eq!(
                parsed.buckets.bucket.last().unwrap().creation_date,
                "2023-06-04T20:17:47.152Z"
            );
        }

        #[test]
        pub fn parse_list_buckets_response_when_no_buckets_exist() {
            let response = r#"
            <?xml version="1.0" encoding="UTF-8"?>
                <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Owner>
                        <ID>02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4</ID>
                        <DisplayName>minio</DisplayName>
                    </Owner>
                    <Buckets>
                    </Buckets>
                </ListAllMyBucketsResult>
            "#;

            let parsed = quick_xml::de::from_str::<super::ListBucketsResponse>(response).unwrap();

            assert_eq!(parsed.owner.display_name, "minio");
            assert_eq!(
                parsed.owner.id,
                "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
            );
            assert_eq!(parsed.buckets.bucket.len(), 0);
        }
    }
}
