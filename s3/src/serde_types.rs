#[derive(Deserialize, Debug)]
pub struct InitiateMultipartUploadResponse {
    #[serde(rename = "Bucket")]
    _bucket: String,
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
    pub display_name: Option<String>,
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
    pub e_tag: Option<String>,
    #[serde(rename = "StorageClass")]
    /// STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER
    pub storage_class: Option<String>,
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

/// An individual upload in a `ListMultipartUploadsResult`
#[derive(Deserialize, Debug, Clone)]
pub struct MultipartUpload {
    #[serde(rename = "Initiated")]
    /// Date and time the multipart upload was initiated
    pub initiated: String,
    #[serde(rename = "StorageClass")]
    /// STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER
    pub storage_class: String,
    #[serde(rename = "Key")]
    /// The object's key
    pub key: String,
    #[serde(rename = "Owner")]
    /// Bucket owner
    pub owner: Option<Owner>,
    #[serde(rename = "UploadId")]
    /// The identifier of the upload
    pub id: String,
}

use std::fmt;

impl fmt::Display for CompleteMultipartUploadData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = String::new();
        for part in self.parts.clone() {
            parts.push_str(&part.to_string())
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

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<Part>").expect("Can't fail");
        write!(f, "<PartNumber>{}</PartNumber>", self.part_number).expect("Can't fail");
        write!(f, "<ETag>{}</ETag>", self.etag).expect("Can't fail");
        write!(f, "</Part>")
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct BucketLocationResult {
    #[serde(rename = "$value")]
    pub region: String,
}

/// The parsed result of a s3 bucket listing
///
/// This accepts the ListBucketResult format returned for both ListObjects and ListObjectsV2
#[derive(Deserialize, Debug, Clone)]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    /// Name of the bucket.
    pub name: String,
    #[serde(rename = "Delimiter")]
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    /// Sets the maximum number of keys returned in the response body.
    pub max_keys: Option<i32>,
    #[serde(rename = "Prefix")]
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: Option<String>,
    #[serde(rename = "ContinuationToken")] // for ListObjectsV2 request
    #[serde(alias = "Marker")] // for ListObjects request
    /// Indicates where in the bucket listing begins. It is included in the response if
    /// it was sent with the request.
    pub continuation_token: Option<String>,
    #[serde(rename = "EncodingType")]
    /// Specifies the encoding method to used
    pub encoding_type: Option<String>,
    #[serde(
        default,
        rename = "IsTruncated",
        deserialize_with = "super::deserializer::bool_deserializer"
    )]
    ///  Specifies whether (true) or not (false) all of the results were returned.
    ///  If the number of results exceeds that specified by MaxKeys, all of the results
    ///  might not be returned.

    /// When the response is truncated (that is, the IsTruncated element value in the response
    /// is true), you can use the key name in in 'next_continuation_token' as a marker in the
    /// subsequent request to get next set of objects. Amazon S3 lists objects in UTF-8 character
    /// encoding in lexicographical order.
    pub is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)] // for ListObjectsV2 request
    #[serde(alias = "NextMarker")] // for ListObjects request
    pub next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    /// Metadata about each object returned.
    pub contents: Vec<Object>,
    #[serde(rename = "CommonPrefixes", default)]
    /// All of the keys rolled up into a common prefix count as a single return when
    /// calculating the number of returns.
    pub common_prefixes: Option<Vec<CommonPrefix>>,
}

/// The parsed result of a s3 bucket listing of uploads
#[derive(Deserialize, Debug, Clone)]
pub struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket")]
    /// Name of the bucket.
    pub name: String,
    #[serde(rename = "NextKeyMarker")]
    /// When the response is truncated (that is, the IsTruncated element value in the response
    /// is true), you can use the key name in this field as a marker in the subsequent request
    /// to get next set of objects. Amazon S3 lists objects in UTF-8 character encoding in
    /// lexicographical order.
    pub next_marker: Option<String>,
    #[serde(rename = "Prefix")]
    /// The prefix, present if the request contained a prefix too, shows the search root for the
    /// uploads listed in this structure.
    pub prefix: Option<String>,
    #[serde(rename = "KeyMarker")]
    /// Indicates where in the bucket listing begins.
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
    #[serde(rename = "Upload", default)]
    /// Metadata about each upload returned.
    pub uploads: Vec<MultipartUpload>,
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

// Taken from https://github.com/rusoto/rusoto
#[derive(Deserialize, Debug, Default, Clone)]
pub struct HeadObjectResult {
    #[serde(rename = "AcceptRanges")]
    /// Indicates that a range of bytes was specified.
    pub accept_ranges: Option<String>,
    #[serde(rename = "CacheControl")]
    /// Specifies caching behavior along the request/reply chain.
    pub cache_control: Option<String>,
    #[serde(rename = "ContentDisposition")]
    /// Specifies presentational information for the object.
    pub content_disposition: Option<String>,
    #[serde(rename = "ContentEncoding")]
    /// Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    pub content_encoding: Option<String>,
    #[serde(rename = "ContentLanguage")]
    /// The language the content is in.
    pub content_language: Option<String>,
    #[serde(rename = "ContentLength")]
    /// Size of the body in bytes.
    pub content_length: Option<i64>,
    #[serde(rename = "ContentType")]
    /// A standard MIME type describing the format of the object data.
    pub content_type: Option<String>,
    #[serde(rename = "DeleteMarker")]
    /// Specifies whether the object retrieved was (true) or was not (false) a Delete Marker.
    pub delete_marker: Option<bool>,
    #[serde(rename = "ETag")]
    /// An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL.
    pub e_tag: Option<String>,
    #[serde(rename = "Expiration")]
    /// If the object expiration is configured, the response includes this header. It includes the expiry-date and rule-id key-value pairs providing object expiration information.
    /// The value of the rule-id is URL encoded.
    pub expiration: Option<String>,
    #[serde(rename = "Expires")]
    /// The date and time at which the object is no longer cacheable.
    pub expires: Option<String>,
    #[serde(rename = "LastModified")]
    /// Last modified date of the object
    pub last_modified: Option<String>,
    #[serde(rename = "Metadata", default)]
    /// A map of metadata to store with the object in S3.
    pub metadata: Option<::std::collections::HashMap<String, String>>,
    #[serde(rename = "MissingMeta")]
    /// This is set to the number of metadata entries not returned in x-amz-meta headers. This can happen if you create metadata using an API like SOAP that supports more flexible metadata than
    /// the REST API. For example, using SOAP, you can create metadata whose values are not legal HTTP headers.
    pub missing_meta: Option<i64>,
    #[serde(rename = "ObjectLockLegalHoldStatus")]
    /// Specifies whether a legal hold is in effect for this object. This header is only returned if the requester has the s3:GetObjectLegalHold permission.
    /// This header is not returned if the specified version of this object has never had a legal hold applied.
    pub object_lock_legal_hold_status: Option<String>,
    #[serde(rename = "ObjectLockMode")]
    /// The Object Lock mode, if any, that's in effect for this object.
    pub object_lock_mode: Option<String>,
    #[serde(rename = "ObjectLockRetainUntilDate")]
    /// The date and time when the Object Lock retention period expires.
    /// This header is only returned if the requester has the s3:GetObjectRetention permission.
    pub object_lock_retain_until_date: Option<String>,
    #[serde(rename = "PartsCount")]
    /// The count of parts this object has.
    pub parts_count: Option<i64>,
    #[serde(rename = "ReplicationStatus")]
    /// If your request involves a bucket that is either a source or destination in a replication rule.
    pub replication_status: Option<String>,
    #[serde(rename = "RequestCharged")]
    pub request_charged: Option<String>,
    #[serde(rename = "Restore")]
    /// If the object is an archived object (an object whose storage class is GLACIER), the response includes this header if either the archive restoration is in progress or an archive copy is already restored.
    /// If an archive copy is already restored, the header value indicates when Amazon S3 is scheduled to delete the object copy.
    pub restore: Option<String>,
    #[serde(rename = "SseCustomerAlgorithm")]
    /// If server-side encryption with a customer-provided encryption key was requested, the response will include this header confirming the encryption algorithm used.
    pub sse_customer_algorithm: Option<String>,
    #[serde(rename = "SseCustomerKeyMd5")]
    /// If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide round-trip message integrity verification of the customer-provided encryption key.
    pub sse_customer_key_md5: Option<String>,
    #[serde(rename = "SsekmsKeyId")]
    /// If present, specifies the ID of the AWS Key Management Service (AWS KMS) symmetric customer managed customer master key (CMK) that was used for the object.
    pub ssekms_key_id: Option<String>,
    #[serde(rename = "ServerSideEncryption")]
    /// If the object is stored using server-side encryption either with an AWS KMS customer master key (CMK) or an Amazon S3-managed encryption key,
    /// The response includes this header with the value of the server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).
    pub server_side_encryption: Option<String>,
    #[serde(rename = "StorageClass")]
    /// Provides storage class information of the object. Amazon S3 returns this header for all objects except for S3 Standard storage class objects.
    pub storage_class: Option<String>,
    #[serde(rename = "VersionId")]
    /// Version of the object.
    pub version_id: Option<String>,
    #[serde(rename = "WebsiteRedirectLocation")]
    /// If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata.
    pub website_redirect_location: Option<String>,
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
