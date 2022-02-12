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
	cd s3; cargo fmt

region-fmt:
	cd aws-region; cargo fmt

creds-fmt:
	cd aws-creds; cargo fmt

s3-clippy:
	cd s3; make clippy

region-clippy:
	cd aws-region; cargo clippy --all-features

creds-clippy:
	cd aws-creds; cargo clippy --all-features


