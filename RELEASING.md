# Release order and dependency compatibility

Carry the changes and contributor acknowledgements in [RELEASE_NOTES.md](RELEASE_NOTES.md)
into the next published release notes.

The `aws-region`, `aws-creds`, and `rust-s3` crates are published separately.
Release `aws-region` 0.29.0 and `aws-creds` 0.40.0 first, and verify both
versions are available from crates.io before releasing `rust-s3` 0.38.0. The
S3 manifest keeps workspace paths for local integration while requiring the
same published dependency versions for consumers.

The `aws-region` 0.29.0 release adds variants to the public exhaustive
`Region` enum. Downstream exhaustive matches must handle the added regions.

This dependency change also changes concrete public error payload types:
`CredentialsError::SerdeXml` exposes `quick_xml::de::DeError`, while
`S3Error::SerdeXml` and `S3Error::XmlSeError` expose quick-xml error types.
Although the type names remain available, Rust treats types from different
quick-xml crate versions as distinct. Users who construct or annotate these
payloads may need to update their dependency and code. `CredentialsError` is
exhaustive, so its payload change is especially visible. The 0.x minor version
bumps communicate that compatibility impact; do not obscure it with aliases.

The `rust-s3` 0.38.0 release also fixes multipart-upload pagination by carrying
both the key marker and upload-ID marker. This changes the public
`Command::ListMultipartUploads` fields, adds marker fields to
`ListMultipartUploadsResult`, and adds an `upload_id_marker` argument to
`Bucket::list_multiparts_uploads_page`. Callers that construct or destructure
these public types, or call the page method directly, need to supply or accept
the new optional marker. Directory buckets and compatible services may omit
the upload-ID marker; continue with the returned key marker alone.

The 0.38.0 release also adds bucket-policy operations and corresponding
variants to the public `Command` enum. Downstream code with exhaustive
`Command` matches must handle `GetBucketPolicy`, `PutBucketPolicy`, and
`DeleteBucketPolicy`.

Before publishing `rust-s3`, confirm the registry contains `aws-creds` 0.40.0
and run package verification against that registry release. Until then, local
path-based compilation validates workspace integration but cannot establish
that the published dependency graph resolves for `rust-s3` consumers.

The credential-refresh repair also changes `aws-creds` source compatibility.
`Credentials` gains private refresh metadata, so external struct literals must
use an existing constructor instead; public credential and expiration fields
remain accessible. The new `CredentialsError::NoRefreshSource` variant requires
updating exhaustive error matches.

Serialized credentials retain their five-field format, but serialization does
not persist a refresh source. Expired manually constructed or deserialized
credentials therefore return `NoRefreshSource` instead of consulting the default
provider chain. Credentials created from a directly supplied OIDC token have the
same behavior: the library does not retain that bearer token. Callers must
explicitly acquire fresh credentials. Token-file credentials reread the captured
file; container and instance-metadata credentials refresh through their captured
provider mechanism. This preserves the source, not an immutable remote IAM
principal if the provider's configuration changes.
