# Keep CI, matrix, and integration phases serial: concurrent feature builds
# contend for the shared target directory and can hit the same providers.
.NOTPARALLEL: ci ci-tokio ci-async-std ci-sync test-all test-all-features \
	test-not-ignored test-ignored test-docs clippy clippy-all tokio-clippy \
	async-std-clippy sync-clippy tokio-test-not-ignored async-std-test-not-ignored \
	sync-test-not-ignored tokio async-std async-all sync-all \
	async-std-fail-on-err-test sync-fail-on-err-test

.PHONY: ci ci-tokio ci-async-std ci-sync test-all test-not-ignored test-ignored \
	test-docs test-all-features async-all sync-all sync-test-not-ignored \
	clippy clippy-all tokio-clippy async-std-clippy sync-clippy \
	tokio-test-not-ignored tokio-test-docs tokio tokio-nativetls-test \
	tokio-nativetls-test-not-ignored tokio-nativetls-test-ignored \
	tokio-nativetls-blocking-test-ignored tokio-nossl-test \
	tokio-nossl-test-not-ignored tokio-rustlstls-test \
	tokio-rustlstls-test-not-ignored tokio-rustlstls-test-ignored \
	async-std async-std-test async-std-test-not-ignored async-std-test-docs \
	async-std-test-ignored async-std-test-blocking-ignored \
	async-std-test-not-ignored-base async-std-test-not-ignored-native-tls \
	async-std-test-ignored-native-tls async-std-test-blocking-ignored-native-tls \
	async-std-native-tls-test async-std-test-not-ignored-rustls-tls \
	async-std-test-ignored-rustls-tls async-std-test-blocking-ignored-rustls-tls \
	async-std-fail-on-err-test \
	async-std-rustls-tls-test sync-nativetls sync-nativetls-clippy \
	sync-nativetls-test sync-nativetls-test-not-ignored sync-nativetls-test-ignored \
	sync-rustlstls sync-rustlstls-clippy sync-rustlstls-test \
	sync-rustlstls-test-not-ignored sync-rustlstls-test-ignored sync-nossl \
	sync-nossl-clippy sync-nossl-test sync-nossl-test-not-ignored \
	sync-fail-on-err-test sync-test-docs fmt fmt-check \
	tokio-nativetls-clippy tokio-nossl-clippy tokio-rustlstls-clippy \
	async-std-clippy-base async-std-clippy-native-tls async-std-clippy-rustls-tls

# Full S3 CI: formatting, all nine library configurations, representative docs,
# and clippy. Provider integration tests remain opt-in through test-ignored.
ci: fmt-check clippy-all test-not-ignored

# Runtime jobs used by CI. Each retains all three TLS modes for its runtime.
ci-tokio:
	$(MAKE) tokio-clippy
	$(MAKE) tokio-test-not-ignored
	$(MAKE) tokio-test-docs

ci-async-std:
	$(MAKE) async-std-clippy
	$(MAKE) async-std-test-not-ignored
	$(MAKE) async-std-test-docs

ci-sync:
	$(MAKE) sync-clippy
	$(MAKE) sync-test-not-ignored
	$(MAKE) sync-test-docs

# Test targets only. `test-all` adds the credentialed provider tests without
# repeating formatting or clippy.
test-all: test-all-features
test-all-features: test-not-ignored test-ignored
test-not-ignored: tokio-test-not-ignored async-std-test-not-ignored sync-test-not-ignored test-docs
test-ignored: tokio-nativetls-test-ignored tokio-nativetls-blocking-test-ignored \
	tokio-rustlstls-test-ignored async-std-test-ignored async-std-test-blocking-ignored \
	async-std-test-ignored-native-tls async-std-test-blocking-ignored-native-tls \
	async-std-test-ignored-rustls-tls async-std-test-blocking-ignored-rustls-tls \
	sync-nativetls-test-ignored sync-rustlstls-test-ignored

test-docs: tokio-test-docs async-std-test-docs sync-test-docs
tokio-test-docs:
	cargo test --doc
	cargo test --doc --features blocking
async-std-test-docs:
	cargo test --doc --no-default-features --features async-std-native-tls,tags,blocking
sync-test-docs:
	cargo test --doc --no-default-features --features sync-native-tls,tags

# Clippy for every runtime/TLS combination.
clippy: clippy-all
clippy-all: tokio-clippy async-std-clippy sync-clippy
tokio-clippy: tokio-nativetls-clippy tokio-nossl-clippy tokio-rustlstls-clippy
async-std-clippy: async-std-clippy-base async-std-clippy-native-tls async-std-clippy-rustls-tls
sync-clippy: sync-nativetls-clippy sync-nossl-clippy sync-rustlstls-clippy

# Tokio library tests: native TLS (default features), no TLS, and rustls.
tokio-test-not-ignored: tokio-nativetls-test-not-ignored tokio-nossl-test-not-ignored tokio-rustlstls-test-not-ignored
tokio: tokio-test-not-ignored
tokio-nativetls-test: tokio-nativetls-test-not-ignored
tokio-nativetls-clippy:
	cargo clippy -- -D warnings
tokio-nativetls-test-not-ignored:
	cargo test --tests --examples
tokio-nativetls-test-ignored:
	cargo test -- --ignored
tokio-nativetls-blocking-test-ignored:
	cargo test --features blocking -- --ignored

tokio-nossl-test: tokio-nossl-test-not-ignored
tokio-nossl-clippy:
	cargo clippy --no-default-features --features with-tokio --features aws-creds/http-credentials -- -D warnings
tokio-nossl-test-not-ignored:
	cargo test --tests --examples --no-default-features --features with-tokio --features aws-creds/http-credentials

tokio-rustlstls-test: tokio-rustlstls-test-not-ignored
tokio-rustlstls-clippy:
	cargo clippy --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials -- -D warnings
tokio-rustlstls-test-not-ignored:
	cargo test --tests --examples --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials
tokio-rustlstls-test-ignored:
	cargo test --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials -- --ignored

# async-std library tests: base/no TLS, native TLS, and rustls.
async-std-test-not-ignored: async-std-test-not-ignored-base async-std-test-not-ignored-native-tls async-std-test-not-ignored-rustls-tls async-std-fail-on-err-test
async-std: async-std-test-not-ignored
async-all: tokio async-std
async-std-test: async-std-test-not-ignored
async-std-clippy-base:
	cargo clippy --no-default-features --features with-async-std-hyper --features aws-creds/http-credentials -- -D warnings
async-std-test-not-ignored-base:
	cargo test --tests --examples --no-default-features --features with-async-std-hyper --features aws-creds/http-credentials
async-std-test-ignored:
	cargo test --no-default-features --features with-async-std-hyper --features aws-creds/http-credentials -- --ignored
async-std-test-blocking-ignored:
	cargo test --no-default-features --features with-async-std-hyper --features blocking --features aws-creds/http-credentials -- --ignored

async-std-native-tls-test: async-std-test-not-ignored-native-tls
async-std-clippy-native-tls:
	cargo clippy --no-default-features --features async-std-native-tls --features aws-creds/http-credentials -- -D warnings
async-std-test-not-ignored-native-tls:
	cargo test --tests --examples --no-default-features --features async-std-native-tls --features aws-creds/http-credentials
async-std-fail-on-err-test:
	cargo test --lib --no-default-features --features async-std-native-tls,fail-on-err,aws-creds/http-credentials xml_response_embedded_error_
	cargo test --lib --no-default-features --features async-std-native-tls,fail-on-err,aws-creds/http-credentials multipart_stream_errors_attempt_abort_and_preserve_primary_error
async-std-test-ignored-native-tls:
	cargo test --no-default-features --features async-std-native-tls --features aws-creds/http-credentials -- --ignored
async-std-test-blocking-ignored-native-tls:
	cargo test --no-default-features --features async-std-native-tls --features blocking --features aws-creds/http-credentials -- --ignored

async-std-rustls-tls-test: async-std-test-not-ignored-rustls-tls
async-std-clippy-rustls-tls:
	cargo clippy --no-default-features --features async-std-rustls-tls --features aws-creds/http-credentials -- -D warnings
async-std-test-not-ignored-rustls-tls:
	cargo test --tests --examples --no-default-features --features async-std-rustls-tls --features aws-creds/http-credentials
async-std-test-ignored-rustls-tls:
	cargo test --no-default-features --features async-std-rustls-tls --features aws-creds/http-credentials -- --ignored
async-std-test-blocking-ignored-rustls-tls:
	cargo test --no-default-features --features async-std-rustls-tls --features blocking --features aws-creds/http-credentials -- --ignored

# Sync library tests: native TLS, rustls, and no TLS. Doctests are run once
# below for the supported API shape rather than once per TLS backend.
sync-test-not-ignored: sync-nativetls-test-not-ignored sync-rustlstls-test-not-ignored sync-nossl-test-not-ignored sync-fail-on-err-test
sync-fail-on-err-test:
	cargo test --lib --no-default-features --features sync,sync-native-tls,fail-on-err,aws-creds/http-credentials xml_response_embedded_error_
	cargo test --lib --no-default-features --features sync,sync-native-tls,fail-on-err,aws-creds/http-credentials multipart_stream_errors_attempt_abort_and_preserve_primary_error
sync-all: sync-test-not-ignored
sync-nativetls: sync-nativetls-test
sync-nativetls-clippy:
	cargo clippy --no-default-features --features sync --features sync-native-tls --features aws-creds/http-credentials -- -D warnings
sync-nativetls-test: sync-nativetls-test-not-ignored
sync-nativetls-test-not-ignored:
	cargo test --tests --examples --no-default-features --features sync --features sync-native-tls --features aws-creds/http-credentials
sync-nativetls-test-ignored:
	cargo test --no-default-features --features sync --features sync-native-tls --features aws-creds/http-credentials -- --ignored

sync-rustlstls: sync-rustlstls-test
sync-rustlstls-clippy:
	cargo clippy --no-default-features --features sync --features sync-rustls-tls --features aws-creds/http-credentials -- -D warnings
sync-rustlstls-test: sync-rustlstls-test-not-ignored
sync-rustlstls-test-not-ignored:
	cargo test --tests --examples --no-default-features --features sync --features sync-rustls-tls --features aws-creds/http-credentials
sync-rustlstls-test-ignored:
	cargo test --no-default-features --features sync --features sync-rustls-tls --features aws-creds/http-credentials -- --ignored

sync-nossl: sync-nossl-test
sync-nossl-clippy:
	cargo clippy --no-default-features --features sync --features aws-creds/http-credentials -- -D warnings
sync-nossl-test: sync-nossl-test-not-ignored
sync-nossl-test-not-ignored:
	cargo test --tests --examples --no-default-features --features sync --features aws-creds/http-credentials

fmt:
	cargo fmt
fmt-check:
	cargo fmt --all -- --check
