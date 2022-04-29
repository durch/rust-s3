async-all: tokio async-std
sync-all: sync
test-all: fmt-check async-all sync-all
ci: clippy fmt-check tokio-not-ignored async-std-test-not-ignored

clippy: tokio-clippy async-std-clippy sync-clippy
tokio-clippy: tokio-noclient-clippy reqwest-clippy
sync-clippy: sync-noclient-clippy attohttpc-clippy

# tokio
tokio: tokio-noclient reqwest
tokio-not-ignored: tokio-noclient-test-not-ignored reqwest-test-not-ignored

tokio-noclient: tokio-noclient-test-not-ignored tokio-noclient-clippy
tokio-noclient-clippy:
	cargo clippy --no-default-features --features with-tokio -- -D warnings
tokio-noclient-test-not-ignored:
	cargo test --no-default-features --features with-tokio

reqwest: reqwest-clippy reqwest-test-not-ignored request-test-blocking-ignored
reqwest-clippy:
	cargo clippy --no-default-features --features with-reqwest -- -D warnings
reqwest-test-not-ignored:
	cargo test --no-default-features --features with-reqwest
request-test-blocking-ignored:
	cargo test --no-default-features --features with-reqwest,blocking -- --ignored

# async-std
async-std: async-std-clippy async-std-test async-std-clippy surf
async-std-clippy:
	cargo clippy --no-default-features --features with-async-std -- -D warnings
async-std-test: async-std-test-not-ignored async-std-test-ignored async-std-test-blocking-ignored
async-std-test-not-ignored:
	cargo test --no-default-features --features with-async-std
async-std-test-ignored:
	cargo test --no-default-features --features with-async-std -- --ignored
async-std-test-blocking-ignored:
	cargo test --no-default-features --features with-async-std --features blocking -- --ignored

surf: surf-clippy surf-test-not-ignored surf-test-blocking-ignored
surf-clippy:
	cargo clippy --no-default-features --features with-surf -- -D warnings
surf-test-not-ignored:
	cargo test --no-default-features --features with-surf
surf-test-blocking-ignored:
	cargo test --no-default-features --features with-surf,blocking -- --ignored

# sync
sync: sync-noclient attohttpc
sync-noclient: sync-noclient-clippy
sync-noclient-clippy:
	cargo clippy --no-default-features --features sync -- -D warnings

attohttpc: attohttpc-clippy attohttpc-test-not-ignored
attohttpc-clippy:
	cargo clippy --no-default-features --features with-attohttpc -- -D warnings
attohttpc-test-not-ignored:
	cargo test --no-default-features --features with-attohttpc

fmt: 
	cargo fmt

fmt-check:
	cargo fmt --all -- --check


