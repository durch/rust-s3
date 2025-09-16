all: ci-all

# Main CI targets - fmt and clippy first, then tests
ci: fmt clippy test

ci-all: fmt clippy test-all

# Formatting targets
fmt: s3-fmt region-fmt creds-fmt

# Clippy targets for all features
clippy: s3-clippy region-clippy creds-clippy

# Test targets (run after fmt and clippy)
test: s3-test region-test creds-test

test-all: s3-test-all region-test creds-test

# Test targets for individual crates
s3-test:
	cd s3; make test-not-ignored

s3-test-all:
	cd s3; make test-all

region-test:
	cd aws-region; cargo test

creds-test:
	cd aws-creds; cargo test

s3-fmt:
	cd s3; cargo fmt --all

region-fmt:
	cd aws-region; cargo fmt --all

creds-fmt:
	cd aws-creds; cargo fmt --all

s3-clippy:
	cd s3; make clippy

region-clippy:
	cd aws-region; cargo clippy --all-features

creds-clippy:
	cd aws-creds; cargo clippy --all-features

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
