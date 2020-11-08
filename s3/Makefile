test: test-not-ignored-async

test-all: test-async test-sync

ci: fmt-check clippy test

fmt: 
	cargo fmt

fmt-check:
	cargo fmt --all -- --check

clippy: clippy-async clippy-sync

clippy-async:
	cargo clippy -- -D warnings

clippy-sync:
	cargo clippy --features sync --no-default-features -- -D warnings

test-not-ignored-async:
	cargo test

test-async: test-not-ignored-async
	cargo test -- --ignored

test-sync:
	cargo test --features sync --no-default-features -- --ignored




