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

// Taken from https://github.com/rusoto/rusoto
#[derive(Deserialize, Debug, Default, Clone)]
pub struct HeadObjectResult {
    /// <p>Indicates that a range of bytes was specified.</p>
    pub accept_ranges: Option<String>,
    /// <p>Specifies caching behavior along the request/reply chain.</p>
    pub cache_control: Option<String>,
    /// <p>Specifies presentational information for the object.</p>
    pub content_disposition: Option<String>,
    /// <p>Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.</p>
    pub content_encoding: Option<String>,
    /// <p>The language the content is in.</p>
    pub content_language: Option<String>,
    /// <p>Size of the body in bytes.</p>
    pub content_length: Option<i64>,
    /// <p>A standard MIME type describing the format of the object data.</p>
    pub content_type: Option<String>,
    /// <p>Specifies whether the object retrieved was (true) or was not (false) a Delete Marker. If false, this response header does not appear in the response.</p>
    pub delete_marker: Option<bool>,
    /// <p>An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL.</p>
    pub e_tag: Option<String>,
    /// <p>If the object expiration is configured (see PUT Bucket lifecycle), the response includes this header. It includes the expiry-date and rule-id key-value pairs providing object expiration information. The value of the rule-id is URL encoded.</p>
    pub expiration: Option<String>,
    /// <p>The date and time at which the object is no longer cacheable.</p>
    pub expires: Option<String>,
    /// <p>Last modified date of the object</p>
    pub last_modified: Option<String>,
    /// <p>A map of metadata to store with the object in S3.</p>
    pub metadata: Option<::std::collections::HashMap<String, String>>,
    /// <p>This is set to the number of metadata entries not returned in <code>x-amz-meta</code> headers. This can happen if you create metadata using an API like SOAP that supports more flexible metadata than the REST API. For example, using SOAP, you can create metadata whose values are not legal HTTP headers.</p>
    pub missing_meta: Option<i64>,
    /// <p>Specifies whether a legal hold is in effect for this object. This header is only returned if the requester has the <code>s3:GetObjectLegalHold</code> permission. This header is not returned if the specified version of this object has never had a legal hold applied. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a>.</p>
    pub object_lock_legal_hold_status: Option<String>,
    /// <p>The Object Lock mode, if any, that's in effect for this object. This header is only returned if the requester has the <code>s3:GetObjectRetention</code> permission. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a>. </p>
    pub object_lock_mode: Option<String>,
    /// <p>The date and time when the Object Lock retention period expires. This header is only returned if the requester has the <code>s3:GetObjectRetention</code> permission.</p>
    pub object_lock_retain_until_date: Option<String>,
    /// <p>The count of parts this object has.</p>
    pub parts_count: Option<i64>,
    /// <p>Amazon S3 can return this header if your request involves a bucket that is either a source or destination in a replication rule.</p> <p>In replication, you have a source bucket on which you configure replication and destination bucket where Amazon S3 stores object replicas. When you request an object (<code>GetObject</code>) or object metadata (<code>HeadObject</code>) from these buckets, Amazon S3 will return the <code>x-amz-replication-status</code> header in the response as follows:</p> <ul> <li> <p>If requesting an object from the source bucket — Amazon S3 will return the <code>x-amz-replication-status</code> header if the object in your request is eligible for replication.</p> <p> For example, suppose that in your replication configuration, you specify object prefix <code>TaxDocs</code> requesting Amazon S3 to replicate objects with key prefix <code>TaxDocs</code>. Any objects you upload with this key name prefix, for example <code>TaxDocs/document1.pdf</code>, are eligible for replication. For any object request with this key name prefix, Amazon S3 will return the <code>x-amz-replication-status</code> header with value PENDING, COMPLETED or FAILED indicating object replication status.</p> </li> <li> <p>If requesting an object from the destination bucket — Amazon S3 will return the <code>x-amz-replication-status</code> header with value REPLICA if the object in your request is a replica that Amazon S3 created.</p> </li> </ul> <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Replication</a>.</p>
    pub replication_status: Option<String>,
    pub request_charged: Option<String>,
    /// <p>If the object is an archived object (an object whose storage class is GLACIER), the response includes this header if either the archive restoration is in progress (see <a>RestoreObject</a> or an archive copy is already restored.</p> <p> If an archive copy is already restored, the header value indicates when Amazon S3 is scheduled to delete the object copy. For example:</p> <p> <code>x-amz-restore: ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"</code> </p> <p>If the object restoration is in progress, the header returns the value <code>ongoing-request="true"</code>.</p> <p>For more information about archiving objects, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html#lifecycle-transition-general-considerations">Transitioning Objects: General Considerations</a>.</p>
    pub restore: Option<String>,
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header confirming the encryption algorithm used.</p>
    pub sse_customer_algorithm: Option<String>,
    /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide round-trip message integrity verification of the customer-provided encryption key.</p>
    pub sse_customer_key_md5: Option<String>,
    /// <p>If present, specifies the ID of the AWS Key Management Service (AWS KMS) symmetric customer managed customer master key (CMK) that was used for the object.</p>
    pub ssekms_key_id: Option<String>,
    /// <p>If the object is stored using server-side encryption either with an AWS KMS customer master key (CMK) or an Amazon S3-managed encryption key, the response includes this header with the value of the server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).</p>
    pub server_side_encryption: Option<String>,
    /// <p>Provides storage class information of the object. Amazon S3 returns this header for all objects except for S3 Standard storage class objects.</p> <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage Classes</a>.</p>
    pub storage_class: Option<String>,
    /// <p>Version of the object.</p>
    pub version_id: Option<String>,
    /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata.</p>
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
