async-all: tokio async-std
sync-all: sync-nativetls sync-rustlstls sync-nossl
test-all: fmt-check async-all sync-all
ci: clippy fmt-check tokio-not-ignored async-std-test-not-ignored

clippy: tokio-clippy async-std-clippy sync-clippy
tokio-clippy: tokio-nativetls-clippy tokio-nossl-clippy tokio-rustlstls-clippy
sync-clippy: sync-nativetls-clippy sync-nossl-clippy sync-rustlstls-clippy

# tokio
tokio: tokio-nativetls tokio-nossl tokio-rustlstls
tokio-not-ignored: tokio-nativetls-test-not-ignored tokio-nossl-test-not-ignored tokio-rustlstls-test-not-ignored

tokio-nativetls: tokio-nativetls-clippy tokio-nativetls-test tokio-nativetls-blocking-test-ignored
tokio-nativetls-clippy:
	cargo clippy -- -D warnings
tokio-nativetls-test: tokio-nativetls-test-not-ignored tokio-nativetls-test-ignored
tokio-nativetls-test-not-ignored:
	cargo test
tokio-nativetls-test-ignored:
	cargo test -- --ignored
tokio-nativetls-blocking-test-ignored:
	cargo test --features blocking -- --ignored

tokio-nossl: tokio-nossl-test-not-ignored tokio-nossl-clippy
tokio-nossl-clippy:
	cargo clippy --no-default-features --features with-tokio -- -D warnings
tokio-nossl-test-not-ignored:
	cargo test --no-default-features --features with-tokio

tokio-rustlstls: tokio-rustlstls-test-not-ignored tokio-rustlstls-test-ignored tokio-rustlstls-clippy
tokio-rustlstls-clippy:
	cargo clippy --no-default-features --features with-tokio --features tokio-rustls-tls -- -D warnings
tokio-rustlstls-test-not-ignored:
	cargo test --no-default-features --features with-tokio --features tokio-rustls-tls
tokio-rustlstls-test-ignored:
	cargo test --no-default-features --features with-tokio --features tokio-rustls-tls -- --ignored

# async-std
async-std: async-std-clippy async-std-test async-std-clippy
async-std-clippy:
	cargo clippy --no-default-features --features with-async-std -- -D warnings
async-std-test: async-std-test-not-ignored async-std-test-ignored async-std-test-blocking-ignored
async-std-test-not-ignored:
	cargo test --no-default-features --features with-async-std
async-std-test-ignored:
	cargo test --no-default-features --features with-async-std -- --ignored
async-std-test-blocking-ignored:
	cargo test --no-default-features --features with-async-std --features blocking -- --ignored

# sync
sync-nativetls: sync-nativetls-clippy sync-nativetls-test
sync-nativetls-clippy:
	cargo clippy --no-default-features --features sync --features sync-native-tls -- -D warnings
sync-nativetls-test: sync-nativetls-test-ignored
sync-nativetls-test-ignored:
	cargo test --no-default-features --features sync --features sync-native-tls -- --ignored

sync-rustlstls: sync-rustlstls-clippy sync-rustlstls-test
sync-rustlstls-clippy:
	cargo clippy --no-default-features --features sync --features sync-rustls-tls -- -D warnings
sync-rustlstls-test: sync-rustlstls-test-ignored
sync-rustlstls-test-ignored:
	cargo test --no-default-features --features sync --features sync-rustls-tls -- --ignored

sync-nossl: sync-nossl-clippy
sync-nossl-clippy:
	cargo clippy --no-default-features --features sync -- -D warnings

fmt: 
	cargo fmt

fmt-check:
	cargo fmt --all -- --check


