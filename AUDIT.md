# Object storage reliability audit

Audit date: 2026-10-02. Baseline: `b584ce7` on `master`, initially clean.

The priority is trustworthy storage behavior across supported runtimes and providers. Expanding the API should follow correctness, bounded resource use, and repeatable compatibility tests.

This is a source review and local test audit, not a security certification or a live provider compatibility claim. No real credentials or remote storage operations were used. Findings below distinguish implemented repairs from open work.

## First repair batch

Implementation is delegated to GPT-6 Luna agents, with parent review and integration.

- Async-std downloads: replace full-body buffering and suppressed read errors with bounded, lazy chunks and error propagation; stream writer downloads incrementally.
- Signature V4 queries: encode each key and value before sorting pairs, preserving duplicate-key ordering and existing signing vectors.
- CI: check formatting without modifying sources, remove duplicate execution, and include nonignored sync tests in the normal test target.

The patches were reviewed and validated locally as recorded below. These repairs do not close the remaining findings. Supporting corrections retain the result of `with_path_style()` in two existing sync tests and remove redundant formatting borrows flagged by current Clippy.

## Findings and repair status

### P1: Resolved dependency graph contains known vulnerabilities and unmaintained backends

`cargo audit` against RustSec checkout `6de4455103aced2cba86e3b86e5c090b22827cf1` reports 30 vulnerability/package matches across 29 advisory IDs, plus 25 warnings, in the existing ignored local `Cargo.lock`. This is a lockfile-wide result, including optional and development dependencies; it is not a count of reachable exploits or a fresh-resolution result for consumers.

Examples include [bytes reserve overflow](https://rustsec.org/advisories/RUSTSEC-2026-0007.html), [time stack exhaustion](https://rustsec.org/advisories/RUSTSEC-2026-0009.html), and quick-xml [namespace allocation](https://rustsec.org/advisories/RUSTSEC-2026-0195.html) and [duplicate-attribute complexity](https://rustsec.org/advisories/RUSTSEC-2026-0194.html). Maintenance warnings cover [async-std](https://rustsec.org/advisories/RUSTSEC-2025-0052.html) and [Surf](https://rustsec.org/advisories/RUSTSEC-2025-0036.html).

Feature-specific `cargo tree` checks confirm that `async-std-rustls-tls` selects rustls 0.18.1 via Surf/http-client/async-tls, and `with-async-std-hyper` selects Hyper 0.13.10 via Surf/http-client. Those older major-version constraints will not all be repaired by refreshing the lockfile.

Next repair: preserve this baseline, resolve and audit a fresh graph separately, classify production feature paths and reachability, and upgrade supported dependencies in tested groups. Establish an explicit maintenance plan for async-std without silently removing a supported runtime. No dependency upgrade or clean security bill is claimed in this batch.

### P1: HTTP 200 can incorrectly report a failed storage operation as successful — repaired in source

At the audit baseline, `Bucket::complete_multipart_upload` returned response data without inspecting its XML root. The high-level streaming upload reduced that response to status and byte count. `Bucket::copy_object` likewise returned only the HTTP status. An embedded error could therefore become apparent success.

AWS explicitly documents this behavior for [multipart completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) and [copy operations](https://repost.aws/knowledge-center/s3-resolve-200-internalerror).

Repair: copy and multipart completion now validate the XML document structure and operation-specific root for 2xx responses. Embedded `<Error>` responses retain the raw body in `HttpFailWithBody`, including service codes and request identifiers present in that body. Invalid/truncated XML and unexpected roots return errors. Whitespace heartbeats, namespace variations, and optional fields are supported. Non-2xx handling remains unchanged, and validation does not add automatic retries. Local tests cover all three runtimes with `fail-on-err` both enabled and disabled; transport interruption and real-provider coverage remain outstanding. No general response-body rule was added to unrelated operations.

### P1: Credential debug output exposes secrets — local credential crate repaired; release pending

At the audit baseline, `aws-creds/src/credentials.rs` derived `Debug` for `Credentials` and `StsResponseCredentials`, including secret keys and session tokens. `Bucket` also derives `Debug` over its credential storage. This is a disclosure path if consumers log these types; this audit did not inspect or find an actual secret leak.

Repair: local `aws-creds` now manually redacts key and token values in `Debug`, including nested STS credential output, while retaining field presence and expiration. Synthetic sentinel tests cover normal/pretty formatting and unchanged serialization. Credential resolution is unchanged. Release coordination remains necessary: `rust-s3` still uses the published `aws-creds`, so this local repair does not yet fix `Bucket` debug output through that dependency.

### P1: Multipart failures can leave incomplete uploads behind

In `_put_object_stream_with_content_type_and_headers`, reader errors and upload errors return through `?` before reaching abort handling. With `fail-on-err`, HTTP errors also arrive as `Err`, bypassing the non-2xx response branch. Cancellation is another unhandled lifecycle boundary.

Next repair: define cleanup behavior for reader failure, part failure, completion failure, and caller cancellation; preserve the original failure if abort also fails. Test abort counts and in-flight requests with a deterministic local server. Do not promise that an async network abort can always complete from `Drop`.

### P1: Large uploads silently lose custom headers

The small-object branch applies `custom_headers` to the PUT builder. The multipart branch calls `initiate_multipart_upload` and `make_multipart_request` without forwarding them. Metadata, storage class, and encryption settings can therefore change at the 8 MiB threshold.

Next repair: classify which headers belong on initiation versus upload parts, then test both sides of the threshold. Passing every header to every multipart operation would be incorrect.

### P2: Multipart pagination drops the upload-ID cursor

`ListMultipartUploadsResult` models `NextKeyMarker` but not `NextUploadIdMarker`. The command and page API likewise omit the upload-ID marker. The aggregate method advances by key only, so it can skip remaining uploads for the same key when a page boundary falls within that key.

The [AWS pagination contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html) requires both markers for general-purpose buckets. Directory buckets differ.

Next repair: preserve both cursor components while considering semver impact of changing public structs/enums. Test multiple uploads for one key spanning pages, missing markers, and nonadvancing cursors.

### P2: Upload concurrency follows machine memory, not a caller budget

`calculate_max_concurrent_chunks` documents a maximum of 10 but clamps to 100. Each chunk is 8 MiB, so one upload can hold roughly 800 MiB of payload before transport copies and overhead. Multiple uploads independently use the same machine-wide memory estimate.

Next repair: establish a conservative per-upload bound and a simple explicit configuration surface only if needed. Measure peak resident memory and throughput with concurrent uploads and slow readers/writers. Do not infer acceptable performance from unit tests.

### P2: Retry policy lacks error and operation classification

`retry!` retries every error up to a global count, using deterministic quadratic delays. It has no transient/permanent classification, jitter, or per-operation replay policy. Backends also expose different error detail (`HttpFail` versus status/body), making consistent decisions harder.

Next repair: use a local fault-injection server to establish behavior for permission failures, throttling, disconnects before/after request transmission, and non-idempotent operations. Keep replay safety separate from transport failures; preserve the final service error.

### P2: Credential refresh can block async execution and change identity

`Bucket::credentials_refresh` calls synchronous credential refresh while holding an async write lock. `Credentials::refresh` reloads `Credentials::default()` only after expiration, rather than retaining the originating provider/profile. Refresh can therefore block runtime workers and resolve through a different source.

Next repair: define provider identity and refresh behavior, test expiry margins and concurrent refresh, and isolate blocking I/O. This requires a deliberate credential design change and release coordination, not an opportunistic reorder of the provider chain.

### P2: Workspace tests do not prove local credentials/region integration

`s3/Cargo.toml` depends on registry versions of `aws-creds` and `aws-region`; the local path alternatives are commented out. `cargo test --workspace` tests local crates individually while `rust-s3` links registry copies. A local provider fix can pass its own tests without being used by the S3 tests.

Next repair: choose and document a workspace dependency/release strategy. Validate the published dependency graph as well as workspace integration. Do not treat a local crate patch as shipped in `rust-s3`.

### P2: Timeout behavior differs by backend and API

`set_request_timeout` only changes the public field; the cached Tokio client retains its configuration. `with_request_timeout` rebuilds the Tokio client, while the Surf request path does not consult the bucket timeout. Existing documentation also refers to an older Hyper backend and inconsistent defaults.

Next repair: specify the timeout contract before changing the infallible setter or client rebuild behavior. Verify delayed headers, stalled bodies, and streaming duration using local fixtures for each backend.

## Further coverage needed

- Signing: repeated header values and whitespace, dot-segment object keys, reserved characters in upload/version identifiers, and presigned requests using temporary credentials. Query ordering is the only signing repair in this batch.
- XML: fuzz representative list, error, tagging, lifecycle, and multipart responses; distinguish malformed/truncated responses from empty results.
- Features: document supported combinations, test blocking wrappers and `tags`/`fail-on-err` independently, and establish a tested minimum Rust version. Mutually exclusive runtime features make a blanket `--all-features` check unsuitable for the main crate.
- Sync documentation: the first batch exposed 29 doctest compile failures and temporarily limited sync CI to `--lib`. Resolved in the test-suite follow-up below, which restores doctests and example compilation.
- Providers: deterministic local HTTP fault tests on each change; scheduled disposable MinIO tests; explicitly authorized AWS/R2/GCS/Wasabi smoke tests before compatibility claims.
- Performance: record throughput, allocations, peak memory, cancellation latency, and connection reuse for concurrent small/large operations. This audit does not establish benchmark results.
- Release discipline: check dependencies against a current advisory database, publish a compatibility matrix and changelog, and verify examples against the actual released artifacts.

## Verification record: first repair batch

Baseline on Rust 1.98.1, macOS aarch64:

- `cargo test --workspace --lib`: local `aws-creds` 4 passed / 1 ignored; local `aws-region` 4 passed; `rust-s3` 59 passed / 25 ignored.
- Real-service ignored tests were not run.
- The initial `cargo audit` database fetch failed. A separate shallow checkout of the public RustSec database was obtained; `cargo audit --db /tmp/rust-s3-advisory-db-20261002 --no-fetch --json` completed and reported the findings above. The checkout hash is recorded to identify the exact advisory snapshot.

After repairs:

- `cargo fmt --all -- --check` and `git diff --check`: passed.
- Root `make clippy`: passed all nine S3 runtime/TLS configurations and both supporting crates. Two pairs of redundant formatting borrows (async and sync) were removed to satisfy Rust 1.98 Clippy.
- Root `make test`: passed. The normal CI target runs these same formatting, lint, and test stages.

| Configuration | Library tests passed | Doctests passed | Ignored library tests |
| --- | ---: | ---: | ---: |
| Tokio default/native TLS | 60 | 43 | 25 |
| Tokio without TLS | 59 | 41 | 22 |
| Tokio rustls | 60 | 42 | 20 |
| Async-std Hyper | 60 | 41 | 22 |
| Async-std native TLS | 60 | 41 | 22 |
| Async-std rustls | 60 | 41 | 22 |
| Sync native TLS | 54 | Not run | 22 |
| Sync rustls | 54 | Not run | 22 |
| Sync without TLS | 54 | Not run | 22 |
| Local aws-region | 4 | 1 | 0 |
| Local aws-creds | 4 | 3 | 1 |

The new streaming tests cover bounded chunks, lazy reads, error propagation, empty bodies, declared-length limits, and an actual localhost HTTP download to a writer, including writer failure. Signing regression tests cover encoded ordering of reserved/Unicode keys and duplicate-key values, preserve existing AWS signing vectors, and verify decoded query pairs survive a round trip. The streaming and signing regressions failed before their fixes.

No real-service ignored tests, performance benchmarks, or deployed/published artifact tests were run. Advisory reachability was not established. Blocking wrappers, `fail-on-err` across every backend, and sync doctests remain outside the passing matrix above.

## Test-suite follow-up

The second batch makes `make` and `make ci` credential-free defaults, preserves all nine runtime/TLS test configurations, and compiles their examples. Provider tests require explicit opt-in. See [TESTING.md](TESTING.md) for commands and coverage.

Sync and blocking documentation compile failures are repaired. Four representative doctest configurations replace repeated TLS-only documentation builds: Tokio default (43 passed), Tokio blocking (76), async-std blocking with tags (75), and sync with tags (36). This closes the sync doctest gap above. Provider examples remain `no_run`: compilation does not establish runtime correctness of blocking wrappers or provider compatibility.

The localhost 404 fixture now has request/server deadlines and stops modifying global retry state. The credential-profile environment test runs in a bounded child process with synthetic credentials, avoiding process-wide environment mutation. Each fixture passed 20 repeated runs.

Local verification on Rust 1.98.1, macOS aarch64:

- `make ci` passed formatting, all nine S3 Clippy/test/example configurations, four doctest configurations, and both support crates (90.30 seconds, including rebuilding changed artifacts).
- Warm `make test` passed in 38.25 seconds; a subsequent `make -j8 test` passed in 23.90 seconds. Make serializes the shared build phases; `-j8` is not evidence of parallel test acceleration.
- The earlier, narrower suite at `9212e43` took 19.28 seconds in an uncontended warm run. The broader suite is locally slower in these measurements; no speedup is claimed. Timings are individual observations, not a controlled benchmark.
- Workflow validation with actionlint and `git diff --check` passed. Existing unused GCS test-helper warnings remain in the Tokio rustls test build.

GitHub CI now separates three runtime jobs and one support job, adds compiler/dependency-scoped caches, timeouts, and cancellation of superseded runs. Hosted execution and its speed remain unverified. Real-provider tests were not run. Remaining work includes runtime blocking-wrapper tests, backend fault coverage, provider fixtures, and measured hosted CI latency; dependency and production-behavior findings above remain open.

## Correctness follow-up verification

The next batch repairs embedded HTTP 200 errors and local credential debug formatting as described above. Implementation was delegated to GPT-6 Luna, then reviewed and integrated by the parent.

- The local HTTP regression failed with the original copy/completion call-site behavior: copy reported success for HTTP 200 `<Error>`. Restoring the validator made the regression pass. The fixture also checks valid copy/completion responses, multipart error bodies with whitespace heartbeats, and malformed XML returned over complete HTTP framing.
- Credential sentinel tests failed with the original derived formatter and pass with redaction. Tests use only synthetic values; serialization and provider resolution are unchanged.
- `make ci` passed on Rust 1.98.1, macOS aarch64, in 56.24 seconds including changed-artifact compilation: all nine S3 Clippy/test/example configurations, two focused `fail-on-err` runs, all four doctest configurations, and both supporting crates.
- S3 library counts are 63 passed for default Tokio, 62 for Tokio without TLS, 63 for Tokio rustls and each async-std configuration, and 57 for each sync configuration. Both additional `fail-on-err` runs execute three matching tests. Doctest counts remain 43/76/75/36. Local `aws-creds` now has eight passing library tests and one ignored test; its three doctests pass.
- A subsequent warm `make test` passed in 16.19 seconds. This is one local observation with more coverage than earlier runs, not a controlled speed comparison or a hosted CI measurement.
- Formatting and diff checks passed; independent review found no blocking issues. No real-provider tests, transport-interruption tests, publication, or hosted workflow execution was performed. Header-only request identifiers are not added to the existing body error type; identifiers in the service XML remain available.

## Order of work

1. Land the reviewed streaming, signing query, and CI repairs with regression evidence.
2. Triage dependency advisories, release and integrate credential redaction, and address multipart cleanup and header preservation in separate changes. Source fixes for false-success responses and local credential formatting are recorded above.
3. Resolve cursor correctness, retry semantics, refresh identity, and timeout parity with a shared local fault-test suite.
4. Establish provider compatibility and performance baselines before expanding API coverage.
