# Object storage reliability audit

Audit date: 2026-10-02. Baseline: `b584ce7` on `master`, initially clean.

The priority is trustworthy storage behavior across supported runtimes and providers. Expanding the API should follow correctness, bounded resource use, and repeatable compatibility tests.

This began as a source review and local test audit. Subsequent MinIO verification and authorized cloud-provider attempts are recorded separately below; neither establishes a security certification or complete provider compatibility. Findings distinguish implemented repairs from open work.

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


#### Fresh dependency resolution, 2026-10-02

An isolated source snapshot at `6c97cb9` resolved 414 packages without changing
the working checkout's ignored lockfile or copying credentials. Both graphs were
audited against freshly fetched RustSec database
`db663534ae858abb3fbad408a041ce04209c377f` (1,279 advisories): the fresh graph
has 12 vulnerability matches across 12 IDs, versus 30 matches across 29 IDs in
the preserved baseline. Thus 18 matches disappear through fresh compatible
resolution; no production dependency upgrade was applied here.

Normal-dependency inverse trees across 11 feature configurations locate the
remaining affected packages:

- `quick-xml 0.38.4`: all configurations; the two recorded advisories require
  `>=0.41.0`, outside the current `0.38` constraint.
- `h2 0.2.7`, `hyper 0.13.10`, and `tokio 0.2.25`: async-std's Hyper configuration.
- `rustls 0.18.1`, `ring 0.16.20`, and `webpki 0.21.4`: async-std's rustls configuration.

This establishes inclusion in production dependency graphs, not exploitability
of each vulnerable code path. Fresh-graph warnings are separate: one notice,
11 unmaintained matches across 10 packages, and five unsoundness matches across
four packages. Surf/http-client's existing backend constraints prevent a simple
compatible version bump from removing the old HTTP/TLS stacks. A no-TLS HTTP/1
feature-tree probe removed the Hyper branch, but that alternative has not been
compiled or compatibility-tested. No backend replacement is claimed.

### P1: HTTP 200 can incorrectly report a failed storage operation as successful — repaired in source

At the audit baseline, `Bucket::complete_multipart_upload` returned response data without inspecting its XML root. The high-level streaming upload reduced that response to status and byte count. `Bucket::copy_object` likewise returned only the HTTP status. An embedded error could therefore become apparent success.

AWS explicitly documents this behavior for [multipart completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) and [copy operations](https://repost.aws/knowledge-center/s3-resolve-200-internalerror).

Repair: copy and multipart completion now validate the XML document structure and operation-specific root for 2xx responses. Embedded `<Error>` responses retain the raw body in `HttpFailWithBody`, including service codes and request identifiers present in that body. Invalid/truncated XML and unexpected roots return errors. Whitespace heartbeats, namespace variations, and optional fields are supported. Non-2xx handling remains unchanged, and validation does not add automatic retries. Local tests cover all three runtimes with `fail-on-err` both enabled and disabled. Successful copy and multipart operations also pass against local MinIO as recorded below; transport interruption and hosted-provider coverage remain outstanding. No general response-body rule was added to unrelated operations.

### P1: Credential debug output exposes secrets — local credential crate repaired; release pending

At the audit baseline, `aws-creds/src/credentials.rs` derived `Debug` for `Credentials` and `StsResponseCredentials`, including secret keys and session tokens. `Bucket` also derives `Debug` over its credential storage. This is a disclosure path if consumers log these types; this audit did not inspect or find an actual secret leak.

Repair: local `aws-creds` now manually redacts key and token values in `Debug`, including nested STS credential output, while retaining field presence and expiration. Synthetic sentinel tests cover normal/pretty formatting and unchanged serialization. Credential resolution is unchanged. Release coordination remains necessary: `rust-s3` still uses the published `aws-creds`, so this local repair does not yet fix `Bucket` debug output through that dependency.

### P1: Multipart failures can leave incomplete uploads behind — repaired in source

At the baseline, high-level streaming uploads could return from reader, part,
ETag parsing, or completion errors without aborting the multipart upload. An
abort error could also replace the original failure, and non-2xx completion
could return apparent success with `fail-on-err` disabled.

After successful initiation, returned failures now take one logical best-effort
abort path and preserve the original error. Existing transport retries may make
more than one wire request. Async part futures are dropped before cleanup; sync
streaming uses a private raw part sender to avoid duplicating the public chunk
helper's abort. Its small-file fallback retains its deliberate abort and existing
status-return behavior. Public signatures and standalone multipart helpers are
unchanged. Completion requires a 2xx status and the existing success-XML check.

A bounded local fixture covers reader errors, failed parts, HTTP completion
errors, embedded Error XML in HTTP 200, abort failure preserving the reader
error, and sync fallback without a second abort. It checks request sequences,
accounts for configured retries without changing global state, and remains
available until the client returns to catch extra requests. All three runtimes
passed with `fail-on-err` both enabled and disabled. Bypassing cleanup made the
test fail on the missing abort; restoring it passed.

This does not guarantee remote cleanup after cancellation or lost responses.
Dropping the caller's future cannot run asynchronous cleanup, and requests
already sent can still be processed. AWS documents that
[in-flight parts may require repeated aborts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html).
The tests do not simulate concurrent in-flight part cancellation or every
transport/ETag failure. Full local and provider follow-up is recorded below.

### P1: Large uploads silently lose custom headers — repaired and provider-verified

At the baseline, the small-object branch applied builder `custom_headers`, while
the multipart branch forwarded none of them. Metadata, cache policy, storage
class, encryption settings, and write conditions could silently change at 8 MiB.

The repair privately routes per-call headers by multipart operation. Object
properties and unknown provider-specific headers go to initiation. SSE-C headers
are carried through initiation, parts, and completion; expected-owner and
requester-pays controls also reach abort. `If-Match` and `If-None-Match` go to
completion. The explicit content-type argument remains authoritative, matching
the existing small-PUT behavior; completion retains its XML content type.

Per-call body framing and whole-object checksum headers that cannot safely be
reused for streamed parts now return `UnsupportedMultipartHeader` before
initiation. The error contains only the header name. Small PUTs and bucket-global
extra-header behavior are unchanged. Global extras remain caller-controlled;
the per-call routing policy does not validate or reinterpret them.

The policy follows AWS's [multipart initiation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html),
[part upload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html),
and [completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
contracts. It does not add streaming checksum computation or change public
Command variants or method signatures. Local and provider verification is recorded below.

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

### P2: R2 rejects generated body headers on bodyless requests — repaired and provider-verified

Live R2 tests reproduced HTTP 403 on range GET in Tokio, async-std, and sync,
after successful PUT, ordinary GET, and existence checks. The shared header
builder generated and signed `Content-Length: 0`; R2's error body showed that
header's value missing from its canonical request. Omitting generated body
headers for ranges allowed both range reads to pass, then exposed the same
problem on DELETE.

The repair omits generated body headers for GET, HEAD, and DELETE commands,
which all have empty request bodies in the current command model. Caller
headers, byte ranges, URL construction, and the signing algorithm are unchanged.
PUT and XML POST body headers and the existing CopyObject special case are
preserved. This follows the recommendation for bodyless requests in
[RFC 9110 section 8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6).
Cross-runtime regressions cover ranges, DELETE, signed-header membership, and
body-bearing operations. The post-repair provider matrix passed as recorded below.

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

## MinIO provider verification

On 2026-10-02, the installed MinIO `RELEASE.2025-10-15T17-29-55Z`
(`9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, darwin/arm64) was started on
loopback using temporary data, certificates, and generated credentials. Tests
used its dedicated `rust-s3` bucket and an empty temporary working directory.
No existing provider credentials or storage were used.

The existing five default-Tokio MinIO tests passed first. Luna then strengthened
test assertions: nonuniform 20 MB data and exact writer/stream contents, stream
item errors and offsets, sorted tag readback, UUID-scoped copy/content checks,
and actual delete status checks in the blocking helper. Production code was
unchanged in this batch.

| Configuration | MinIO tests passed |
| --- | ---: |
| Tokio native TLS / no TLS / rustls | 5 / 5 / 5 |
| async-std Hyper / native TLS / rustls | 5 / 5 / 5 |
| Sync native TLS / rustls / no TLS | 5 / 5 / 5 |
| Tokio native TLS with blocking | 6 |
| async-std native TLS with blocking | 6 |

All 57 strengthened test executions passed, with `tags` enabled in every
configuration. Each invocation was restricted to `minio --ignored
--test-threads=1`. Aggregate test execution was 26.81 seconds; sequential build
and test time was 65.82 seconds on this machine. These are suite timings, not
storage throughput benchmarks.

The normal `make ci` gate also passed after the test changes (46.89 seconds,
including changed-artifact compilation), as did formatting and diff checks.

Post-test listings confirmed zero objects and zero incomplete multipart uploads,
with neither listing truncated. The test bucket was deleted successfully; the
owned server stopped, temporary data and credentials were removed, and port
9000 was released. [TESTING.md](TESTING.md) describes the isolated procedure.

This establishes local MinIO success-path coverage, including real blocking API
execution. It does not establish TLS handshake coverage (the endpoint used
HTTP), cloud-provider compatibility, or cleanup after failure/cancellation.
Cloud credentials were not loaded for that MinIO run. The later cloud attempts
are recorded below. The production findings above remain open.

## Cloud-provider verification attempt

The user subsequently authorized loading the existing `.envrc` credentials.
Values were used privately without changing the file. Authenticated HEAD,
object/multipart listing, and versioning checks succeeded against the existing
test buckets on AWS, Wasabi, GCS, R2, and DigitalOcean. Versioning was disabled.
These curl checks establish endpoint access, not rust-s3 compatibility.

Luna added `RUST_S3_TEST_PREFIX` isolation to the object fixtures and restored or
added DigitalOcean CRUD/multipart, Wasabi multipart, and R2 blocking wrappers.
AWS tag readback now asserts exact tags. Selected tests used unique prefixes
and empty temporary working directories. Bucket configuration/ACL tests were
excluded; only the selected run's objects and uploads were eligible for cleanup.

The initial cloud library attempts stalled:

- AWS default Tokio CRUD exceeded a 180-second process deadline. Sync native TLS
  failed its first PUT with a connection timeout after 66 seconds. Tokio rustls
  also exceeded 180 seconds; temporary stage logging located it at the first PUT.
- Wasabi default Tokio CRUD was interrupted after approximately 100 seconds.
  The remaining provider/runtime matrix was not executed.
- A temporary unsigned reqwest GET to the AWS bucket endpoint also timed out
  after five seconds, reproducing the problem without signing or credentials.
- Signed curl PUT/DELETE probes to the same AWS virtual host succeeded, including
  the fixture's 3 KB payload and encoded `+test.file` suffix.
- Little Snitch is running on the host. A per-executable network restriction is
  a hypothesis awaiting user confirmation; no firewall settings were changed
  and no library transport defect is claimed from these observations.

Independent post-attempt listings confirmed zero objects and zero incomplete
uploads under every attempted test prefix. The curl probe objects were deleted
with HTTP 204. Temporary diagnostic Rust code was removed. At that point, GCS Tokio
rustls exclusions remained a coverage gap, not a passing configuration.

Two local `make ci` attempts after prefix changes failed in the XML-response
localhost fixture: first with async-std rustls, then with Tokio rustls. The
affected test passed 20 focused repetitions and the exact full async-std rustls
suite passed 10 repetitions (63 passed, 25 ignored each), but those successes
did not close the recurring full-gate failure.

The follow-up reproduced its cause on macOS: sockets accepted from a nonblocking
listener retained nonblocking mode, so a read before the client sent its first
byte immediately returned `WouldBlock`. Both affected localhost fixtures now
explicitly restore blocking mode on accepted streams while retaining their
read/write and server deadlines. A channel-coordinated delayed-client regression
failed when that setup was removed and passed when restored. The Tokio and
async-std rustls suites then passed with 64 library tests each. The complete
`make ci` gate passed in 59.12 seconds, including all nine S3 configurations,
the additional error-feature checks, four doctest shapes, and both support
crates. This is one local timing observation, not a throughput benchmark.

A subsequent unsigned HTTPS probe returned HTTP 403 from AWS as expected,
so the cloud matrix could resume. No firewall configuration was changed; the
cause of the earlier host-connectivity stall remains unconfirmed.

The default-Tokio rerun then passed all selected tests on AWS (5), Wasabi (2),
GCS (3), and DigitalOcean (2). R2 CRUD failed at the first range GET with HTTP
403 `SignatureDoesNotMatch`; R2's canonical request showed an empty
`Content-Length` value where the shared header builder signed `0`. Native-TLS
async-std and sync runs reproduced the same range failure. Each failed run's
single object was independently removed and both listings were verified empty.
R2 streaming tests and the remaining cloud runtime/TLS matrix were still pending
at that point.

The prefix-enabled MinIO fixtures passed another 57 executions across the same
11 configurations (28.76 aggregate test seconds). The first launch attempt
failed because the harness's server process had exited; the successful rerun
used a persistent foreground process and verified health before tests. Every
successful run had empty, untruncated object and multipart listings. The owned
bucket was deleted (204, followed by 404), the server stopped, and its data and
generated credentials removed. These results precede the R2 range-header repair.


## Completed provider matrix after bodyless-header repair

All 100 selected cloud test executions passed against the configured AWS,
Wasabi, GCS, R2, and DigitalOcean test buckets. The matrix comprises 15 tests
per configuration for Tokio, async-std, and sync with native TLS and rustls
(90), plus one blocking CRUD/range/list-pagination test per provider for each
async runtime (10). GCS Tokio-rustls tests are now enabled and passed. There
were no skipped or uncompiled selections. Independent signed listings verified
zero objects and incomplete uploads under all 40 provider/configuration prefixes.

The first blocking runs passed on four providers but exposed a fixture ordering
assumption on R2. An independent signed curl probe returned `file2,file3` on
page one and `file` on page two, with valid truncation and continuation fields.
The three probe objects were removed and both listings verified empty. The
fixture now verifies exact, duplicate-free keys across both pages while retaining
strict page sizes, status, truncation, and token checks. It does not alter library
ordering or claim that R2 provides lexicographic ordering. All ten cloud blocking
tests passed after this fixture correction.

The production header repair also passed all 57 MinIO executions across 11
configurations (26.95 aggregate test seconds). Both blocking configurations were
rerun after the listing assertion change: another 12 passes. Each prefix was
verified empty; the disposable bucket was deleted (204, followed by 404), the
owned server stopped, and its generated credentials and data removed.

`make ci` passed after the production repair, covering all nine runtime/TLS
configurations, error-feature checks, four doctest shapes, and both support
crates. The subsequent change affected only the ignored blocking fixture; both
blocking configurations were rebuilt and exercised on MinIO and all five cloud
providers. Final formatting and diff checks passed.

These results establish the selected success paths and real TLS connections to
cloud providers. They do not establish failure/cancellation cleanup, exhaustive
S3 API compatibility, throughput benchmarks, dependency remediation, hosted CI,
or release of the local credential fix. Those roadmap items remain open.


## Multipart cleanup verification follow-up

The cleanup repair passed the full `make ci` gate in 205.94 seconds with changed
artifacts rebuilt, and a cached rerun in 99.93 seconds with no compilation steps.
These are local elapsed-time observations; concurrent cloud testing and host
load affect them. All nine runtime/TLS configurations, four doctest shapes,
support crates, and both focused non-default `fail-on-err` targets passed. The
new cleanup checks are retained in those focused CI targets.

The frozen source also passed all 57 MinIO executions across 11 configurations
(30.22 aggregate test seconds), followed by the same 100 cloud executions across
AWS, Wasabi, GCS, R2, and DigitalOcean described above. All 40 cloud prefixes
were independently verified empty. MinIO listings were empty and untruncated;
the bucket was deleted (204, then 404), and the owned server, data, certificates,
and generated credentials were removed. This provider rerun establishes
success-path compatibility after the cleanup change; failure cleanup evidence
comes from the bounded local fault fixtures, not injected cloud failures.


## Multipart header verification follow-up

Tokio and async-std passed synthetic wire tests below, at, and above the multipart
threshold. The tests verify object properties, SSE-C, conditions, owner/payer
controls including abort, generated body headers, bucket-global/per-call
precedence, part numbers independent of arrival order, and rejection before any
request. Disabling routing reproduced the missing per-call metadata at initiation;
restoring it passed. Public sync behavior remains unchanged.

The complete `make ci` gate passed with no warnings in 290.80 seconds, including
rebuilding changed artifacts. The frozen source then passed all 57 MinIO tests
across 11 configurations (31.68 aggregate test seconds) and all 100 cloud tests
across the five providers and eight runtime/TLS/blocking configurations. Async
small and multipart upload fixtures now verify persisted metadata, cache control,
and content type through HEAD, alongside exact payload bytes. Sync fixtures
retain their existing checks.

All 40 cloud prefixes were independently verified empty. MinIO listings were
empty and untruncated, its bucket deletion returned 204 followed by 404, and the
owned server, synthetic credentials, data, and certificates were removed. These
results verify live object-property preservation; real-provider SSE-C and
conditional-write scenarios remain outside the live matrix and have only
synthetic local wire coverage.

## XML dependency and credential integration follow-up

The source now prepares `aws-creds 0.40.0` and `rust-s3 0.38.0`, both using
quick-xml 0.41. S3 uses a path-plus-version dependency on the local credential
crate, so its redacted credential formatting is now exercised through `Bucket`.
A synthetic sentinel regression checks both ordinary and pretty Debug output.
The XML adapter preserves the previous text-normalization behavior and uses the
new decoder-aware attribute API. The full `make ci` gate passed without warnings.

An isolated fresh resolution audited against the same RustSec database
`db663534ae858abb3fbad408a041ce04209c377f` reports 10 vulnerability matches across
10 IDs, down from 12: both quick-xml advisories are removed. The remaining matches
are in h2, Hyper, Tokio 0.2, ring, rustls 0.18, and webpki, associated with the
legacy async-std transport branches described above. Separate warnings remain:
one notice, 11 unmaintained matches, and five unsoundness matches. This is not a
clean security assessment or a claim that every listed code path is exploitable.

`aws-creds` package creation and verification passed. S3 package listing passed,
but registry-based packaging cannot resolve the new credential version until it
is published. Neither crate has been published. [RELEASING.md](RELEASING.md)
records the required release order and public quick-xml error-type compatibility
impact behind the minor version bumps.

The frozen dependency patch passed 57 MinIO tests across 11 configurations
(28.12 aggregate test seconds) and all 100 selected cloud tests across AWS,
Wasabi, GCS, R2, and DigitalOcean. All 40 cloud prefixes were independently
verified empty, as were MinIO object and upload listings. The isolated MinIO
server is retained temporarily for the next pagination regression.

## Order of work

1. Land the reviewed streaming, signing query, and CI repairs with regression evidence.
2. Remediate the remaining dependency advisories, release and integrate credential redaction, and address the remaining lifecycle and API findings in separate changes. Source fixes for false-success responses and local credential formatting are recorded above.
3. Resolve cursor correctness, retry semantics, refresh identity, and timeout parity with a shared local fault-test suite.
4. Establish provider compatibility and performance baselines before expanding API coverage.
