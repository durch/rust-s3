all: ci-all

ci: s3-ci region-ci creds-ci

ci-all: s3-all region-ci creds-ci

fmt: s3-fmt region-fmt creds-fmt

clippy: s3-clippy region-clippy creds-clippy

s3-all:
	cd s3; make test-all

s3-ci:
	cd s3; make ci

region-ci:
	cd aws-region; make ci

creds-ci:
	cd aws-creds; make ci

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
