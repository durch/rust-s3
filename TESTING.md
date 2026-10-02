# Test and CI coverage

`make` and `make ci` run the full credential-free local suite: format check,
clippy, all nine S3 runtime/TLS library and example configurations, representative
API doctests, and the `aws-region` and `aws-creds` test suites. Ignored provider
tests are opt-in through `make integration-test` or `make test-ignored` from
`s3/`. `make test-all` and `make ci-all` include those integrations and require
the corresponding provider or credential setup.

The S3 feature matrix uses `cargo test --tests --examples` so each configuration
checks the library tests and compiles its examples. TLS implementations are
covered separately from doctests because the TLS choice does not change the
generated public API. Representative doctest commands also keep coverage for
the default `tags` feature and for the `blocking` API:

| Runtime | TLS configuration | Feature selection | Coverage |
| --- | --- | --- | --- |
| Tokio | native TLS | default features | library tests and examples; default feature set includes `tags` |
| Tokio | no TLS | `with-tokio`, `aws-creds/http-credentials` | library tests and examples |
| Tokio | rustls | `with-tokio`, `tokio-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| async-std | base/no TLS | `with-async-std-hyper`, `aws-creds/http-credentials` | library tests and examples |
| async-std | native TLS | `async-std-native-tls`, `aws-creds/http-credentials` | library tests and examples |
| async-std | rustls | `async-std-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | native TLS | `sync`, `sync-native-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | rustls | `sync`, `sync-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | no TLS | `sync`, `aws-creds/http-credentials` | library tests and examples |

Doctests run for default Tokio, Tokio with `blocking`, async-std native TLS with
`tags` and `blocking`, and sync native TLS with `tags`. The sync documentation
examples are compiled in that representative configuration; the other sync TLS
variants still run their library and example coverage.

The nine configurations cover `fail-on-err` disabled across all three
runtimes. Default Tokio native TLS covers it enabled for Tokio. Two focused
library-test commands also enable `fail-on-err` for async-std native TLS and
sync native TLS, using the `xml_response_embedded_error_` and
`multipart_stream_errors_attempt_abort_and_preserve_primary_error` filters.
These checks cover embedded response errors and multipart cleanup without
repeating the full runtime matrix.

The GitHub workflow runs formatting and support-crate checks once, then runs
three bounded S3 jobs for Tokio, async-std, and sync. Each runtime job covers all
three TLS configurations. Cargo registry and build caches are scoped by runtime,
resolved workspace manifest, and Rust compiler fingerprint; cache paths exclude
Cargo configuration and credentials. The repository ignores `Cargo.lock`, so
CI resolves dependencies at run time and does not promise a fixed dependency
snapshot across workflow runs.

## Isolated MinIO provider tests

The ignored MinIO tests expect `http://localhost:9000`, an existing bucket named
`rust-s3`, and `MINIO_ACCESS_KEY_ID` / `MINIO_SECRET_ACCESS_KEY`. Use a disposable
MinIO instance with its own data and certificate directories, bound to loopback,
and temporary credentials. Do not point these fixed-name fixtures at a shared
bucket. The default test suite does not start a server.

Build the test executable from the repository with
`cargo test -p rust-s3 --lib --no-run --message-format=json`, adding
`--no-default-features --features ...` for each configuration. Use the `s3`
compiler artifact's `executable` path from Cargo's JSON output. Run that executable
from an empty temporary working directory with arguments
`minio --ignored --test-threads=1`, inheriting only the intended test configuration.
The temporary working directory matters: the current multipart fixture creates
and removes a file named `+stream_test_big` in its working directory.

Run configurations serially because the tests share object names. Enable `tags`
in each configuration to include tag readback. The native-TLS Tokio and async-std
configurations can additionally enable `blocking` to exercise the blocking API.
Afterward, verify that both object and multipart-upload listings are empty, delete
the test bucket, stop the owned server process, and remove its temporary data.

The 2026-10-02 local run passed against MinIO `RELEASE.2025-10-15T17-29-55Z`:
five ignored tests in each of nine runtime/TLS feature configurations, plus six
in each of two native-TLS blocking configurations (57 executions). The tests
check CRUD, byte ranges, metadata, copied content, bulk deletion, tag readback,
small streaming uploads, and exact bytes through a 20 MB multipart upload and
download. Async streaming fixtures also verify builder metadata, cache control,
and content type through HEAD on both small and multipart uploads. Async configurations also verify every downloaded stream chunk;
blocking tests include list pagination and deletion status checks.

These are HTTP tests of a real local MinIO server. They do not verify TLS
handshakes, AWS/R2/GCS/Wasabi behavior, or failure/cancellation cleanup. See
[AUDIT.md](AUDIT.md) for remaining coverage and production fixes.

## Cloud object tests

Load only the intended provider credentials into the test process. The existing
fixtures use source-defined test buckets; review those targets before running.
Set `RUST_S3_TEST_PREFIX` to a fresh UUID-based prefix ending in `/`, for example
`rust-s3-audit/<uuid>/tokio/aws/`. Object fixtures prepend it to their keys;
leaving it unset retains their historical fixed keys. Run the compiled test
executable from an empty temporary directory, as for MinIO.

Select exact object tests, for example
`bucket::test::aws_put_head_get_delete_object --ignored --exact --test-threads=1`.
A blanket ignored run also includes bucket-wide configuration tests and is not
an isolated object smoke test. The prefix applies to the object fixtures, not
every ignored test. Serialize tests sharing a prefix and bound each process's
runtime. After failure or timeout, independently list and clean up only that
prefix's objects and multipart uploads, then verify both listings are empty.
Use an unversioned test bucket for this procedure; deleting the current object
does not clean up historical versions in a versioned bucket.

The 2026-10-02 post-repair cloud matrix passed all 100 selected tests:

| Provider | Six runtime/TLS configurations | Two blocking configurations | Total |
| --- | ---: | ---: | ---: |
| AWS | 30 | 2 | 32 |
| Wasabi | 12 | 2 | 14 |
| GCS | 18 | 2 | 20 |
| R2 | 18 | 2 | 20 |
| DigitalOcean | 12 | 2 | 14 |

The six configurations are Tokio, async-std, and sync, each with native TLS and
rustls. The two blocking configurations use Tokio and async-std native TLS.
All selections compiled and ran; all 40 test prefixes were independently
verified empty afterward. GCS tests include Tokio rustls. The blocking fixture
checks exact keys across pages without assuming their order, while retaining
strict pagination assertions; raw R2 responses exhibited a different ordering.

The full local gate and MinIO matrix also passed. See [AUDIT.md](AUDIT.md) for
repairs, historical failures, cleanup evidence, and the remaining limits. This
matrix covers selected object operations, not bucket configuration, every S3
API, failure/cancellation cleanup, or performance benchmarks.

The credential-free suite also captures local HTTP requests below, exactly at,
and above the 8 MiB multipart threshold. It checks per-call header routing,
bucket-header precedence, generated body headers, and abort controls, and verifies
unsupported checksum/framing headers fail before initiation. All values are
synthetic; this does not establish real-provider SSE-C or conditional-write
support. Those header paths have local wire coverage, while provider runs verify
metadata, cache control, content type, and object bytes.
