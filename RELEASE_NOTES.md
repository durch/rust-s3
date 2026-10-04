# Release notes

## rust-s3 0.38.0

This release updates the companion crates to `aws-creds` 0.40.0 and
`aws-region` 0.29.0.

### Highlights

- Added bucket policy get, put, and delete operations.
- Added AWS region identifiers, including newer AWS regions, and the
  `tokio-rustls-tls-ring` feature for using rustls with its Ring crypto
  provider.
- Improved credential-provider support and refresh for web identity, ECS
  container credentials, and EKS Pod Identity. Credential secrets are redacted
  from `Debug` output.
- Made streamed multipart uploads configurable with a per-upload concurrency
  limit, preserved per-call headers, and added best-effort aborts for failed
  uploads.
- Improved request reliability with operation-aware retries, request deadlines,
  bounded async downloads, corrected custom-endpoint authorities and encoded
  query signing, and detection of S3 errors returned inside successful XML
  responses.
- Avoided stale connection reuse in the async-std HTTP/1 transport by disabling
  keep-alive; this can increase connection setup overhead. The Hyper transport
  continues to reuse pooled connections.
- Corrected multipart-upload pagination to carry both S3 markers and fixed
  bodyless request signing for S3-compatible providers.
- Updated XML parsing to quick-xml 0.41, which removes the quick-xml advisories
  identified in the prior dependency audit.

### S3-compatible request signing

Fixed generated `Content-Length` and `Content-Type` headers entering signatures
for bodyless requests, including ranged GET and DELETE operations. This resolves
signature mismatches on providers such as Cloudflare R2. Multipart initiation
retains the headers needed by Google Cloud Storage, and requests with bodies
retain their content headers. The fix is included in this release via
[`6c97cb9`](https://github.com/durch/rust-s3/commit/6c97cb99aa49fe49ecda0c39787b896bf21a6de0).

Thanks to the contributors who investigated this issue and proposed fixes:

- [Piero Molino (@w4nderlust)](https://github.com/w4nderlust) —
  [#452](https://github.com/durch/rust-s3/pull/452), covering ranged GET signing,
  broader body-header handling, and the GCS multipart-initiation exception.
- [Mohamed Dardouri (@dardourimohamed)](https://github.com/dardourimohamed) —
  [#459](https://github.com/durch/rust-s3/pull/459), addressing ranged GET headers.
- [Roman Oswald (@r-oswald)](https://github.com/r-oswald) —
  [#465](https://github.com/durch/rust-s3/pull/465), addressing DeleteObject headers.
- [Eldon Pinheiro (@eldon-databanx)](https://github.com/eldon-databanx) —
  [#467](https://github.com/durch/rust-s3/pull/467), covering bodyless DELETE
  operations and strict signature validation.

Their contributions are acknowledged here because the fixes were consolidated
in the audit work rather than merging these PRs separately.

### Streamed multipart upload headers

Preserved per-call headers across streamed multipart uploads, routing them to
the requests where S3-compatible services expect them. Thanks to
[@mfroembgen](https://github.com/mfroembgen) for PR
[#471](https://github.com/durch/rust-s3/pull/471) and its focused regression
coverage.

### Migration notes

- `Credentials` now contains private refresh-source metadata. External struct
  literals must use a constructor. Manually constructed or deserialized
  credentials, and credentials made from a directly supplied OIDC token, do not
  retain a refresh source; refreshing expired credentials in these cases
  returns `CredentialsError::NoRefreshSource`. Token-file, container, and
  instance-metadata credentials retain their provider refresh behavior. The
  serialized five-field credential format is unchanged. Add handling for the
  new `NoRefreshSource` variant if you exhaustively match `CredentialsError`.
- `aws-region` adds variants to its public exhaustive `Region` enum. Downstream
  exhaustive matches must handle the added regions.
- Multipart-upload listing now carries the key marker and upload-ID marker.
  `Command::ListMultipartUploads` and `ListMultipartUploadsResult` have new
  marker fields, and `Bucket::list_multiparts_uploads_page` takes an additional
  `upload_id_marker` argument. Update code constructing or destructuring these
  public types or calling that method. Services that omit an upload-ID marker
  can continue with the returned key marker alone.
- The public `Command` enum adds `GetBucketPolicy`, `PutBucketPolicy`, and
  `DeleteBucketPolicy`; update exhaustive matches accordingly.
- The quick-xml update changes the concrete quick-xml error payload types in
  `CredentialsError::SerdeXml`, `S3Error::SerdeXml`, and
  `S3Error::XmlSeError`. Rust considers types from different quick-xml versions
  distinct, so code that constructs or annotates these payloads may need to
  update its dependency and type references.

### Dependency advisory status

A dependency audit still reports advisories in legacy optional async-std
transport dependency branches. The audit did not establish whether those
findings are reachable or exploitable; this release is not a clean security
assessment.
