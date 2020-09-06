#[derive(Deserialize, Debug)]
pub struct InitiateMultipartUploadResponse {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// Owner information for the object
#[derive(Deserialize, Debug, Clone)]
pub struct Owner {
    #[serde(rename = "DisplayName")]
    /// Object owner's name.
    pub display_name: String,
    #[serde(rename = "ID")]
    /// Object owner's ID.
    pub id: String,
}

/// An individual object in a `ListBucketResult`
#[derive(Deserialize, Debug, Clone)]
pub struct Object {
    #[serde(rename = "LastModified")]
    /// Date and time the object was last modified.
    pub last_modified: String,
    #[serde(rename = "ETag")]
    /// The entity tag is an MD5 hash of the object. The ETag only reflects changes to the
    /// contents of an object, not its metadata.
    pub e_tag: String,
    #[serde(rename = "StorageClass")]
    /// STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER
    pub storage_class: String,
    #[serde(rename = "Key")]
    /// The object's key
    pub key: String,
    #[serde(rename = "Owner")]
    /// Bucket owner
    pub owner: Option<Owner>,
    #[serde(rename = "Size")]
    /// Size in bytes of the object.
    pub size: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Tagging {
    #[serde(rename = "TagSet")]
    pub tag_set: Vec<Tag>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Tag {
    #[serde(rename = "Tag")]
    pub kvpair: KVPair,
}

impl Tag {
    pub fn key(&self) -> String {
        self.kvpair.key.clone()
    }

    pub fn value(&self) -> String {
        self.kvpair.value.clone()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct KVPair {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

use std::fmt;

impl fmt::Display for CompleteMultipartUploadData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = String::new();
        for part in self.parts.clone() {
            parts.push_str(&serde_xml_rs::to_string(&part).unwrap())
        }
        write!(
            f,
            "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
            parts
        )
    }
}

impl CompleteMultipartUploadData {
    pub fn len(&self) -> usize {
        self.to_string().as_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.to_string().as_bytes().len() == 0
    }
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartUploadData {
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Part {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BucketLocationResult {
    #[serde(rename = "$value")]
    pub region: String,
}

/// The parsed result of a s3 bucket listing
#[derive(Deserialize, Debug, Clone)]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    /// Name of the bucket.
    pub name: String,
    #[serde(rename = "NextMarker")]
    /// When the response is truncated (that is, the IsTruncated element value in the response
    /// is true), you can use the key name in this field as a marker in the subsequent request
    /// to get next set of objects. Amazon S3 lists objects in UTF-8 character encoding in
    /// lexicographical order.
    pub next_marker: Option<String>,
    #[serde(rename = "Delimiter")]
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    /// Sets the maximum number of keys returned in the response body.
    pub max_keys: i32,
    #[serde(rename = "Prefix")]
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: String,
    #[serde(rename = "Marker")]
    /// Indicates where in the bucket listing begins. Marker is included in the response if
    /// it was sent with the request.
    pub marker: Option<String>,
    #[serde(rename = "EncodingType")]
    /// Specifies the encoding method to used
    pub encoding_type: Option<String>,
    #[serde(
        rename = "IsTruncated",
        deserialize_with = "super::deserializer::bool_deserializer"
    )]
    ///  Specifies whether (true) or not (false) all of the results were returned.
    ///  If the number of results exceeds that specified by MaxKeys, all of the results
    ///  might not be returned.
    pub is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    /// Metadata about each object returned.
    pub contents: Vec<Object>,
    #[serde(rename = "CommonPrefixes", default)]
    /// All of the keys rolled up into a common prefix count as a single return when
    /// calculating the number of returns.
    pub common_prefixes: Option<Vec<CommonPrefix>>,
}

/// `CommonPrefix` is used to group keys
#[derive(Deserialize, Debug, Clone)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    /// Keys that begin with the indicated prefix.
    pub prefix: String,
}

#[derive(Deserialize, Debug)]
pub struct AwsError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}
