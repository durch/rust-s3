# Release order and dependency compatibility

The `aws-creds` and `rust-s3` crates are published separately. For the
quick-xml 0.41 transition, release `aws-creds` 0.40.0 first, then verify that
version is available from crates.io before releasing `rust-s3` 0.38.0. The S3
manifest keeps a workspace path for local integration while requiring the same
published `aws-creds` version for consumers.

This dependency change also changes concrete public error payload types:
`CredentialsError::SerdeXml` exposes `quick_xml::de::DeError`, while
`S3Error::SerdeXml` and `S3Error::XmlSeError` expose quick-xml error types.
Although the type names remain available, Rust treats types from different
quick-xml crate versions as distinct. Users who construct or annotate these
payloads may need to update their dependency and code. `CredentialsError` is
exhaustive, so its payload change is especially visible. The 0.x minor version
bumps communicate that compatibility impact; do not obscure it with aliases.

Before publishing `rust-s3`, confirm the registry contains `aws-creds` 0.40.0
and run package verification against that registry release. Until then, local
path-based compilation validates workspace integration but cannot establish
that the published dependency graph resolves for `rust-s3` consumers.
