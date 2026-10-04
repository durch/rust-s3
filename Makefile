# Keep CI and test phases serial against the shared workspace target directory.
.NOTPARALLEL: all ci ci-all ci-support ci-tokio ci-async-std ci-sync \
	clippy test test-all integration-test s3-test s3-test-all

.PHONY: all ci ci-all ci-support ci-tokio ci-async-std ci-sync \
	fmt fmt-check clippy test test-all integration-test s3-test s3-test-all \
	region-test creds-test s3-fmt region-fmt creds-fmt s3-clippy \
	region-clippy creds-clippy example-async-std example-gcs-tokio \
	example-minio example-r2 example-sync example-tokio \
	example-clippy-async-std example-clippy-gcs-tokio example-clippy-minio \
	example-clippy-r2 example-clippy-sync example-clippy-tokio examples-clippy

# Plain `make` is the full credential-free developer and CI path.
all: ci

# Check formatting, lint every S3 runtime/TLS combination, then test all ten
# library/example configurations and representative docs.
ci: fmt-check clippy test

# Full local quality gate for support crates and formatting. GitHub runs this
# once beside the three S3 runtime jobs.
ci-support: fmt-check region-clippy creds-clippy region-test creds-test

# Per-runtime S3 checks used by the bounded CI matrix.
ci-tokio:
	$(MAKE) -C s3 ci-tokio

ci-async-std:
	$(MAKE) -C s3 ci-async-std

ci-sync:
	$(MAKE) -C s3 ci-sync

# The explicit all-tests path may contact configured object storage providers.
ci-all: ci integration-test
test-all: test integration-test

integration-test:
	$(MAKE) -C s3 test-ignored
	cd aws-creds && cargo test -- --ignored

# Formatting targets
fmt: s3-fmt region-fmt creds-fmt
fmt-check:
	cargo fmt --all -- --check

# Clippy and test targets
clippy: s3-clippy region-clippy creds-clippy
test: s3-test region-test creds-test

# Individual crates
s3-test:
	$(MAKE) -C s3 test-not-ignored

s3-test-all:
	$(MAKE) -C s3 test-all

region-test:
	cd aws-region && cargo test

creds-test:
	cd aws-creds && cargo test

s3-fmt:
	cd s3 && cargo fmt --all

region-fmt:
	cd aws-region && cargo fmt --all

creds-fmt:
	cd aws-creds && cargo fmt --all

s3-clippy:
	$(MAKE) -C s3 clippy-all

region-clippy:
	cd aws-region && cargo clippy --all-features

creds-clippy:
	cd aws-creds && cargo clippy --all-features

example-async-std:
	cargo run --example async-std --no-default-features --features async-std-native-tls

example-gcs-tokio:
	cargo run --example google-cloud

example-minio:
	cargo run --example minio

example-r2:
	cargo run --example r2

example-sync:
	cargo run --example sync --no-default-features --features sync-native-tls

example-tokio:
	cargo run --example tokio

example-clippy-async-std:
	cargo clippy --example async-std --no-default-features --features async-std-native-tls

example-clippy-gcs-tokio:
	cargo clippy --example google-cloud

example-clippy-minio:
	cargo clippy --example minio

example-clippy-r2:
	cargo clippy --example r2

example-clippy-sync:
	cargo clippy --example sync --no-default-features --features sync-native-tls

example-clippy-tokio:
	cargo clippy --example tokio

examples-clippy: example-clippy-async-std example-clippy-gcs-tokio example-clippy-minio example-clippy-r2 example-clippy-sync example-clippy-tokio
