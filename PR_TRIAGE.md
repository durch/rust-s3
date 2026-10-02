# rust-s3 PR triage — 2026-10-02

This is a review snapshot, not an execution receipt. No PR was merged, closed,
or commented on. The repository skill requires exact per-PR action approval.

## Grounding and recommendation

Reviewed all 14 open PRs through the authenticated GitHub connector: pinned head
metadata, diffs, discussion, commit checks/statuses, and Actions run lookup.
The CLI read failed authentication; the connector succeeded. All PRs target
`master`, report mergeable, and are non-draft except #460. Mergeability alone
is not readiness. Missing Actions results are **unknown coverage**, not a pass.
The lookup returns only the first workflow page; failed #474/#471 runs returned
no job detail, so neither failure is classified as flaky or unrelated.

Remote master is `b584ce7d53825705332c13136769546166622ad1`; local audited source
is `ef296a02` (21 commits ahead). Local fixes and test results must not be described
as merged upstream or released. No public closure based solely on local overlap.

Prioritize credited integration of **#468 and #452**, then **#477 and #474**,
followed by **#449 and #454**. #471 needs reconciliation with the existing local
multipart repair. #464/#447 need focused behavior coverage; #460/#462 need scope
and verification work. Consolidate #459/#465/#467 only after the broader fix is
publicly integrated. Do not add another broad audit phase before these actions.

No exact PR head meets the skill's complete merge gate yet. `make ci-all` was
not run on these PRs, and this triage did not run real-provider tests. Prior
`make ci` + 68 MinIO + 100 cloud passes apply to the audited local source only.
Focused results below are narrower evidence, not a substitute. Before a merge
recommendation, finish the full gate or obtain explicit acceptance of named
cordons; use disposable resources for bucket-configuration tests.

The approved report correction removes nine status-only comment drafts. Only
concrete author follow-up drafts remain for #447, #454, #460, #462, and #464.
Approving that correction does not approve posting these comments.

## Batch dispositions

| PR | Disposition | Confidence | Risk / sensitive areas | Verification | Approval |
|---|---|---|---|---|---|
| [#477](https://github.com/durch/rust-s3/pull/477) | Follow-up: maintainer verification | medium | High: public Region enum | 15 unit tests and 1 doctest pass at pinned head. No Actions run returned; Reviewable pending. | Internal next action only; no public approval requested |
| [#474](https://github.com/durch/rust-s3/pull/474) | Follow-up: maintainer verification | high | High: Host header and signing | Two helper tests pass on Tokio/rustls at pinned head. GitHub build run 31845930538 failed; jobs API returned no details. | Internal next action only; no public approval requested |
| [#471](https://github.com/durch/rust-s3/pull/471) | Follow-up: maintainer integration | high | High: multipart headers and public API | GitHub build run 30838535828 failed; job details unavailable. Diff includes wire and ignored MinIO boundary tests; not rerun here. | Internal next action only; no public approval requested |
| [#468](https://github.com/durch/rust-s3/pull/468) | Follow-up: maintainer integration/release | high | High: XML dependency and release graph | Manifest diff reviewed; no Actions run returned. Prior local audit validation applies to d8f99e5/ef296a0, not this exact PR head. | Internal next action only; no public approval requested |
| [#467](https://github.com/durch/rust-s3/pull/467) | Close after consolidation into #452 | high | High: bodyless signing | Diff covers six DELETE variants; no tests added. No Actions run returned. | Internal next action only; no public approval requested |
| [#465](https://github.com/durch/rust-s3/pull/465) | Close after consolidation into #452 | high | High: bodyless signing | One-line DeleteObject fix; no tests added. No Actions run returned. | Internal next action only; no public approval requested |
| [#464](https://github.com/durch/rust-s3/pull/464) | Request author follow-up | high | High: multipart resource and error behavior | Actions build passed. Brownian Motion advisory failed; it is not a compiler/test failure. No added tests in diff. | `APPROVE COMMENT #464` for draft below; no merge/close yet |
| [#462](https://github.com/durch/rust-s3/pull/462) | Request author follow-up | high | High: policy API, signing, dependencies | No Actions run returned. No operation tests in diff. | `APPROVE COMMENT #462` for draft below; no merge/close yet |
| [#460](https://github.com/durch/rust-s3/pull/460) | Request author follow-up; retain draft | high | High: dependency/TLS/MSRV sweep | Draft. No Actions run returned; no local build run in this triage. | `APPROVE COMMENT #460` for draft below; no merge/close yet |
| [#459](https://github.com/durch/rust-s3/pull/459) | Close after consolidation into #452 | high | High: ranged GET signing | One-line ranged GET fix; no added tests. No Actions run returned. | Internal next action only; no public approval requested |
| [#454](https://github.com/durch/rust-s3/pull/454) | Request author follow-up | high | High: TLS feature/dependency resolution | Pinned-head offline cargo check fails: registry aws-creds 0.39.1 has no rustls-tls-ring feature. No Actions run returned. | `APPROVE COMMENT #454` for draft below; no merge/close yet |
| [#452](https://github.com/durch/rust-s3/pull/452) | Follow-up: maintainer integration | high | High: signing and provider compatibility | Actions build passed. Current diff and May 6 author follow-up reviewed; no fresh provider execution on this PR head. | Internal next action only; no public approval requested |
| [#449](https://github.com/durch/rust-s3/pull/449) | Follow-up: maintainer integration plus tests | high | High: credentials and token handling | No Actions run returned. Author supplied 13 passing/1 ignored tests in May; not independently rerun here. | Internal next action only; no public approval requested |
| [#447](https://github.com/durch/rust-s3/pull/447) | Request author follow-up | high | High: Linux cgroups and multipart memory | No Actions run returned. Linux-specific code not executed on this macOS host. | `APPROVE COMMENT #447` for draft below; no merge/close yet |

## Per-PR evidence and actionable author follow-ups

### #477 — feat(aws-region): add missing AWS regions

- **Author / base / head:** `nfawcett` / `master` / `5912ab1c7b07864b3cec354c6c67a419d0e3ebed`.
- **Opened / updated:** 2026-09-29 / 2026-09-29.
- **Recommendation / confidence / risk:** Follow-up: maintainer verification; medium; High: public Region enum.
- **Evidence and API/provider impact:** The eleven endpoint mappings match the AWS S3 endpoint table. Existing mapping behavior is preserved, but adding enum variants affects downstream exhaustive matches. S3 still depends on registry aws-region 0.28.1, so workspace region tests do not prove S3 consumes this change.
- **Verification:** 15 unit tests and 1 doctest pass at pinned head. No Actions run returned; Reviewable pending.
- **Concrete next action:** Maintainer: finish the gate, document the enum compatibility change, and coordinate the aws-region release plus S3 dependency update.
- **Public action:** None proposed now; retain the next action internally.

### #474 — fix: keep the endpoint path out of the Host header

- **Author / base / head:** `eastriverlee` / `master` / `786653e1b4abd47cb046ef1227aa646d97073c2c`.
- **Opened / updated:** 2026-08-14 / 2026-08-14.
- **Recommendation / confidence / risk:** Follow-up: maintainer verification; high; High: Host header and signing.
- **Evidence and API/provider impact:** Splitting at the first slash preserves host:port and removes the endpoint path only from Host; request URL construction stays unchanged. Tests cover the helper but do not exercise signed requests or presigned URLs end-to-end. The author reports Supabase and MinIO success; that has not been independently repeated.
- **Verification:** Two helper tests pass on Tokio/rustls at pinned head. GitHub build run 31845930538 failed; jobs API returned no details.
- **Concrete next action:** Maintainer: diagnose or rerun the failed build, add wire/presign checks for endpoint paths and ports, and run runtime/provider verification.
- **Public action:** None proposed now; retain the next action internally.

### #471 — Preserve headers in multipart stream uploads

- **Author / base / head:** `mfroembgen` / `master` / `97f98d0983d0dd5387ef451fa4cb3e3a8002c5d3`.
- **Opened / updated:** 2026-08-03 / 2026-08-03.
- **Recommendation / confidence / risk:** Follow-up: maintainer integration; high; High: multipart headers and public API.
- **Evidence and API/provider impact:** The author already addressed header precedence with a regression and added sync documentation. Local commit 6a3a6b8 independently routes headers at initiation, parts, completion and abort, but is unpublished. The PR also adds a public initiation-with-headers method that is not in the local repair.
- **Verification:** GitHub build run 30838535828 failed; job details unavailable. Diff includes wire and ignored MinIO boundary tests; not rerun here.
- **Concrete next action:** Maintainer: reconcile contributor changes with the broader local header routing, retain credit, choose the smallest public API, and rerun the failed CI/provider boundary cases. Do not repeat resolved review requests.
- **Public action:** None proposed now; retain the next action internally.

### #468 — deps: bump quick-xml to 0.41 to avoid RUSTSEC-2026-019{4,5}

- **Author / base / head:** `aznashwan` / `master` / `01b6ccb9babf858475163a4b9bcd7dea3d56fd00`.
- **Opened / updated:** 2026-07-02 / 2026-09-15.
- **Recommendation / confidence / risk:** Follow-up: maintainer integration/release; high; High: XML dependency and release graph.
- **Evidence and API/provider impact:** The two quick-xml 0.41 changes are already present in unpublished local d8f99e5. That local repair also prepares aws-creds 0.40.0 and S3 0.38.0 and adapts newly added XML validation. The PR is not defective merely because it lacks adapters for our later code. S3 still using registry aws-creds 0.39.1 would retain the older XML dependency until release coordination.
- **Verification:** Manifest diff reviewed; no Actions run returned. Prior local audit validation applies to d8f99e5/ef296a0, not this exact PR head.
- **Concrete next action:** Maintainer: integrate with contributor attribution and complete aws-creds-first release/dependency verification. Do not close as publicly fixed before integration is visible.
- **Public action:** None proposed now; retain the next action internally.

### #467 — fix: do not sign Content-Length/Content-Type on bodyless DELETE requests

- **Author / base / head:** `eldon-databanx` / `master` / `505aded53d2e668bad472bd7834019e8b1f7a4dd`.
- **Opened / updated:** 2026-07-02 / 2026-09-01.
- **Recommendation / confidence / risk:** Close after consolidation into #452; high; High: bodyless signing.
- **Evidence and API/provider impact:** This covers more DELETE commands than #465 but is included by #452 and the unpublished local repair. Recent users still report the bug in released 0.37.2; a local commit does not resolve their issue.
- **Verification:** Diff covers six DELETE variants; no tests added. No Actions run returned.
- **Concrete next action:** After the broader fix is publicly integrated, close as a credited duplicate. No close approval requested yet.
- **Public action:** None proposed now; retain the next action internally.

### #465 — fix: exclude content-length and content-type from DeleteObject signed…

- **Author / base / head:** `r-oswald` / `master` / `d1723983b67727128ecc9555b7d49e99534d064c`.
- **Opened / updated:** 2026-06-02 / 2026-06-24.
- **Recommendation / confidence / risk:** Close after consolidation into #452; high; High: bodyless signing.
- **Evidence and API/provider impact:** The narrow fix is valid in direction and is subsumed by the broader pending #452. Do not close based only on unpublished local 6c97cb9.
- **Verification:** One-line DeleteObject fix; no tests added. No Actions run returned.
- **Concrete next action:** After public integration of the broader fix, close with contributor credit.
- **Public action:** None proposed now; retain the next action internally.

### #464 — make max_concurrent_chunks for PutObjectStreamRequest configurable

- **Author / base / head:** `robinfriedli` / `master` / `caa841cc4e0c843b8f00072d05a2ef8b08ece559`.
- **Opened / updated:** 2026-05-16 / 2026-05-16.
- **Recommendation / confidence / risk:** Request author follow-up; high; High: multipart resource and error behavior.
- **Evidence and API/provider impact:** A NonZeroUsize caller limit is useful and is not replaced by our internal 2..10 clamp. The PR adds a separate sequential implementation and changes multipart control flow without regression coverage. Local abort/error-preservation repairs must survive integration.
- **Verification:** Actions build passed. Brownian Motion advisory failed; it is not a compiler/test failure. No added tests in diff.
- **Concrete next action:** Add tests for limits 1 and >1, default behavior, part order/bytes, chunk boundaries, and read/upload/completion failures on Tokio and async-std; prefer one upload algorithm if it can handle limit 1.
- **Approval:** `APPROVE COMMENT #464` posts only the following draft. It does not authorize a merge or close.

> Thanks for adding caller-controlled concurrency. Please add focused tests for a limit of one, a larger limit, and the default, checking part order and exact bytes across chunk boundaries. Please also cover reader/upload/completion failures and multipart cleanup on Tokio and async-std. If the concurrent helper can safely handle one in-flight part, that would avoid maintaining a separate sequential algorithm.

### #462 — Added bucket policies

- **Author / base / head:** `AndriBaal` / `master` / `0d2b4951b1318d86874b79d284d7bbf371ec9022`.
- **Opened / updated:** 2026-05-14 / 2026-09-03.
- **Recommendation / confidence / risk:** Request author follow-up; high; High: policy API, signing, dependencies.
- **Evidence and API/provider impact:** Bucket-centric policy methods are reasonable, but the PR bundles unrelated reqwest/crypto/XML/sysinfo upgrades and adds no exact request tests. The get method returns String, losing status information with fail-on-err disabled. Review that behavior explicitly rather than silently treating error bodies as policies.
- **Verification:** No Actions run returned. No operation tests in diff.
- **Concrete next action:** Split dependency updates; add verb/query/body/hash/error tests, runtime-compatible examples, and disposable-bucket provider coverage. Do not run policy tests on shared configured buckets.
- **Approval:** `APPROVE COMMENT #462` posts only the following draft. It does not authorize a merge or close.

> Thanks for adding bucket policy operations. Please separate the unrelated dependency upgrades so the API can be reviewed independently. Add tests for GET/PUT/DELETE with ?policy, the exact JSON payload and signing hash, and error responses with fail-on-err both enabled and disabled. In particular, clarify how get_bucket_policy exposes a non-success response when returning String. Please verify the runtime examples and document provider support.

### #460 — Update dependencies

- **Author / base / head:** `LockedThread` / `master` / `1b81656b28e26300d22c320ad6a8e461ad143157`.
- **Opened / updated:** 2026-05-07 / 2026-05-18.
- **Recommendation / confidence / risk:** Request author follow-up; retain draft; high; High: dependency/TLS/MSRV sweep.
- **Evidence and API/provider impact:** Broad reqwest/hmac/sha2/minidom upgrades need a feature and API audit. The PR sets quick-xml 0.39, behind the focused 0.41 security update. The author already documented an unchanged measured MSRV of 1.88; do not ask them to justify it as if absent. This does not by itself remove the legacy Surf dependency advisories.
- **Verification:** Draft. No Actions run returned; no local build run in this triage.
- **Concrete next action:** Narrow/rebase the update after XML integration, retain fixed versions, validate each TLS configuration and claimed MSRV, then mark ready when complete.
- **Approval:** `APPROVE COMMENT #460` posts only the following draft. It does not authorize a merge or close.

> Thanks for documenting the MSRV measurement. Please keep the focused quick-xml 0.41 security update when revising this draft, and separate or clearly validate the reqwest/TLS and crypto upgrades across the supported feature matrix. The older Surf branches need separate remediation, so I would not treat this dependency sweep as clearing the remaining security findings.

### #459 — fix: exclude content-length and content-type from GetObjectRange signed headers

- **Author / base / head:** `dardourimohamed` / `master` / `4c7ed2b44d6fbf1ebdd401dd3a81c14d288cffb2`.
- **Opened / updated:** 2026-05-06 / 2026-05-06.
- **Recommendation / confidence / risk:** Close after consolidation into #452; high; High: ranged GET signing.
- **Evidence and API/provider impact:** This is explicitly offered as a narrow alternative to #452. The broader PR now includes the GCS multipart exception and covers this change.
- **Verification:** One-line ranged GET fix; no added tests. No Actions run returned.
- **Concrete next action:** After public integration of the broader fix, close with credit for the ranged-GET report.
- **Public action:** None proposed now; retain the next action internally.

### #454 — fix: Add new feature flags to aws-creds and rust-s3 to allow for ring support

- **Author / base / head:** `dVeon-loch` / `master` / `9def2583248e9cd72a7002686230bf35d61bd60b`.
- **Opened / updated:** 2026-04-13 / 2026-05-04.
- **Recommendation / confidence / risk:** Request author follow-up; high; High: TLS feature/dependency resolution.
- **Evidence and API/provider impact:** attohttpc 0.30.1 has the requested ring feature. reqwest 0.12.28 rustls-tls selects ring, so the earlier speculative concern about reqwest forcing aws-lc is not supported. The actual blocker is that S3 selects a registry credentials crate rather than the modified workspace crate.
- **Verification:** Pinned-head offline cargo check fails: registry aws-creds 0.39.1 has no rustls-tls-ring feature. No Actions run returned.
- **Concrete next action:** Wire the local crate for verification, coordinate the new credentials release and S3 dependency, add docs and graph/build checks proving no aws-lc in the isolated ring configuration.
- **Approval:** `APPROVE COMMENT #454` posts only the following draft. It does not authorize a merge or close.

> Thanks for the ring feature. The attohttpc feature exists and reqwest 0.12 selects ring on this path. I reproduced a dependency-resolution blocker: S3 still requests published aws-creds 0.39.1, which does not expose rustls-tls-ring. Please wire the modified credentials crate for local verification and coordinate the dependency/release change, then include the two feature-build commands and a dependency-tree check showing the isolated ring configuration excludes aws-lc.

### #452 — Fix ranged GET signing failure with Cloudflare R2

- **Author / base / head:** `w4nderlust` / `master` / `2936eb7e856655a4a49a65377e3cdb59e29fea71`.
- **Opened / updated:** 2026-04-06 / 2026-05-07.
- **Recommendation / confidence / risk:** Follow-up: maintainer integration; high; High: signing and provider compatibility.
- **Evidence and API/provider impact:** The author fixed the previously reported GCS 411 regression by retaining initiation body headers, and includes DeleteObjects. This overlaps unpublished local 6c97cb9. Prefer a credited integration rather than asking the author to repeat completed work or discarding their history.
- **Verification:** Actions build passed. Current diff and May 6 author follow-up reviewed; no fresh provider execution on this PR head.
- **Concrete next action:** Maintainer: reconcile lineage with the local implementation, verify exact final code on GCS/R2 and full matrix, then request exact merge approval.
- **Public action:** None proposed now; retain the next action internally.

### #449 — Add support for AWS_CONTAINER_CREDENTIALS_FULL_URI (EKS Pod Identity Agent)

- **Author / base / head:** `LockedThread` / `master` / `d088ddee024264390947b440f89d9a39af78c2e8`.
- **Opened / updated:** 2026-03-03 / 2026-07-10.
- **Recommendation / confidence / risk:** Follow-up: maintainer integration plus tests; high; High: credentials and token handling.
- **Evidence and API/provider impact:** The current diff addresses the earlier token-destination allowlist, unreadable-file failure, and rustdoc requests. Do not repeat those requests. Remaining tests often assert any network error instead of successful request/refresh behavior. Integrating our unpublished refresh-source model is maintainer work; authors cannot rebase onto unpublished commits.
- **Verification:** No Actions run returned. Author supplied 13 passing/1 ignored tests in May; not independently rerun here.
- **Concrete next action:** Preserve URI/token source through refresh; add a deterministic successful-fetch/refresh fixture, rotated token-file and redirect/header handling checks. Check current SDK compatibility before widening host rules.
- **Public action:** None proposed now; retain the next action internally.

### #447 — fix: use cgroup memory limits for multipart upload concurrency in con…

- **Author / base / head:** `ProbstenHias` / `master` / `e5225f0b152e145c6e6dab8d8590dd5880f97570`.
- **Opened / updated:** 2026-02-12 / 2026-05-04.
- **Recommendation / confidence / risk:** Request author follow-up; high; High: Linux cgroups and multipart memory.
- **Evidence and API/provider impact:** The cgroup test module is Linux-only but imports a helper gated on Linux AND async features; a Linux sync test build would lack that helper. File fixtures repeat parse/subtract logic instead of exercising production readers. The 100-chunk clamp is pre-existing in remote master, as the author said; our unpublished clamp repair is separate.
- **Verification:** No Actions run returned. Linux-specific code not executed on this macOS host.
- **Concrete next action:** Align test cfg gates, test production filesystem readers with fixture roots including fallback behavior, document conservative page-cache handling; integrate with the local bounded cap later.
- **Approval:** `APPROVE COMMENT #447` posts only the following draft. It does not authorize a merge or close.

> Thanks for the parser fixes. Two items remain: the Linux test module imports an async-only helper, so please align the cfg gates and verify a Linux sync test build; and please have the cgroup file fixtures exercise the production readers rather than repeating their arithmetic. Please also document the conservative page-cache choice. The 100-chunk cap was pre-existing, as you noted; I will reconcile it with the separate bounded-concurrency work.

## Commands and remaining verification

Pinned archives were used without changing master. Full command/result logs
and GitHub snapshots are in `/tmp/rust-s3-triage-20261002/`.

- #477: `cargo test -p aws-region` — 15 unit tests, 1 doctest passed.
- #474: `cargo test -p rust-s3 --lib request::request_trait::tests::test_authority_of --no-default-features --features with-tokio,tokio-rustls-tls` — 2 passed. This is helper coverage, not provider verification.
- #454: `cargo check -p rust-s3 --no-default-features --features with-tokio,tokio-rustls-tls-ring --offline` — resolver failure for missing registry aws-creds feature. Offline scope is explicit; it is not evidence that an uninspected future registry release lacks the feature.
- Region mapping reference: [AWS S3 endpoints](https://docs.aws.amazon.com/general/latest/gr/s3.html).
- Confirmed from installed manifests: attohttpc 0.30.1 supports `tls-rustls-webpki-roots-ring`; reqwest 0.12.28's `rustls-tls` enables its ring feature.

Each disposition must be rechecked against the pinned head before posting.
A future merge additionally requires completed verification and `APPROVE MERGE #N`;
a closure requires `APPROVE CLOSE #N`. Preserve original author/commit credit
when reconciling overlapping local work. No release, push, or public PR action
was executed by this triage.
