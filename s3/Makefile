test-all: fmt-check async-all sync-all
async-all: tokio async-std
sync-all: sync-nativetls sync-rustlstls sync-nossl
ci: clippy fmt-check tokio-not-ignored async-std-test-not-ignored

clippy: tokio-clippy async-std-clippy sync-clippy
tokio-clippy: tokio-nativetls-clippy tokio-nossl-clippy tokio-noverify-clippy tokio-rustlstls-clippy
sync-clippy: sync-nativetls-clippy sync-nossl-clippy sync-rustlstls-clippy

# tokio
tokio: tokio-nativetls tokio-nossl tokio-noverify tokio-rustlstls
tokio-not-ignored: tokio-nativetls-test-not-ignored tokio-nossl-test-not-ignored tokio-noverify-test-not-ignored tokio-rustlstls-test-not-ignored

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
	cargo clippy --no-default-features --features with-tokio --features aws-creds/http-credentials -- -D warnings
tokio-nossl-test-not-ignored:
	cargo test --no-default-features --features with-tokio --features aws-creds/http-credentials

tokio-noverify: tokio-noverify-test-not-ignored tokio-noverify-clippy
tokio-noverify-clippy:
	cargo clippy --no-default-features --features with-tokio --features no-verify-ssl --features aws-creds/http-credentials -- -D warnings
tokio-noverify-test-not-ignored:
	cargo test --no-default-features --features with-tokio --features no-verify-ssl --features aws-creds/http-credentials

tokio-rustlstls: tokio-rustlstls-test-not-ignored tokio-rustlstls-test-ignored tokio-rustlstls-clippy
tokio-rustlstls-clippy:
	cargo clippy --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials -- -D warnings
tokio-rustlstls-test-not-ignored:
	cargo test --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials
tokio-rustlstls-test-ignored:
	cargo test --no-default-features --features with-tokio --features tokio-rustls-tls --features aws-creds/http-credentials -- --ignored

# async-std
async-std: async-std-clippy async-std-test async-std-clippy
async-std-clippy:
	cargo clippy --no-default-features --features with-async-std --features aws-creds/http-credentials -- -D warnings
async-std-test: async-std-test-not-ignored async-std-test-ignored async-std-test-blocking-ignored
async-std-test-not-ignored:
	cargo test --no-default-features --features with-async-std --features aws-creds/http-credentials
async-std-test-ignored:
	cargo test --no-default-features --features with-async-std --features aws-creds/http-credentials -- --ignored
async-std-test-blocking-ignored:
	cargo test --no-default-features --features with-async-std --features blocking --features aws-creds/http-credentials -- --ignored

# sync
sync-nativetls: sync-nativetls-clippy sync-nativetls-test
sync-nativetls-clippy:
	cargo clippy --no-default-features --features sync --features sync-native-tls --features aws-creds/http-credentials -- -D warnings
sync-nativetls-test: sync-nativetls-test-ignored
sync-nativetls-test-ignored:
	cargo test --no-default-features --features sync --features sync-native-tls --features aws-creds/http-credentials -- --ignored

sync-rustlstls: sync-rustlstls-clippy sync-rustlstls-test
sync-rustlstls-clippy:
	cargo clippy --no-default-features --features sync --features sync-rustls-tls --features aws-creds/http-credentials -- -D warnings
sync-rustlstls-test: sync-rustlstls-test-ignored
sync-rustlstls-test-ignored:
	cargo test --no-default-features --features sync --features sync-rustls-tls --features aws-creds/http-credentials  -- --ignored

sync-nossl: sync-nossl-clippy
sync-nossl-clippy:
	cargo clippy --no-default-features --features sync --features aws-creds/http-credentials -- -D warnings

fmt: 
	cargo fmt

fmt-check:
	cargo fmt --all -- --check


