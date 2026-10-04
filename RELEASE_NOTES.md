# Release notes

## Unreleased — rust-s3 0.38.0

### S3-compatible request signing

Fixed generated `Content-Length` and `Content-Type` headers entering signatures
for bodyless requests, including ranged GET and DELETE operations. This resolves
signature mismatches on providers such as Cloudflare R2. Multipart initiation
retains the headers needed by Google Cloud Storage, and requests with bodies
retain their content headers. The fix is on `master` in
[`6c97cb9`](https://github.com/durch/rust-s3/commit/6c97cb99aa49fe49ecda0c39787b896bf21a6de0)
and is not yet a published release.

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
