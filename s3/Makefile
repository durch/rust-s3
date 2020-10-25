test: test-async test-sync

ci: test-not-ignored

clippy: clippy-async clippy-sync

clippy-async:
	cargo clippy -- -D warnings

clippy-sync:
	cargo clippy --features sync --no-default-features -- -D warnings

test-not-ignored:
	cargo test

test-async: test-not-ignored
	cargo test -- --ignored

test-sync: test-not-ignored
	cargo test --features sync --no-default-features -- --ignored

# Doc tests for sync will be broken due to maybe_async
# test-sync-ignore:
# 	cargo test --features sync --no-default-features




