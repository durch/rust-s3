# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

rust-s3 is a Rust library for working with Amazon S3 and S3-compatible object storage APIs (Minio, Wasabi, GCS, R2, etc.). It's a workspace project with three main crates:

- **s3** - Main library implementation in `/s3`
- **aws-region** - AWS region handling in `/aws-region`
- **aws-creds** - AWS credentials management in `/aws-creds`

## Development Commands

### Building and Testing

```bash
# Run CI tests (recommended first step)
make ci

# Run all tests including ignored ones
make ci-all

# Format code
make fmt

# Run clippy lints
make clippy

# Test specific runtime configurations
cd s3
make tokio           # Test with tokio runtime
make async-std       # Test with async-std runtime
make sync-nativetls  # Test sync implementation

# Run a single test
cargo test test_name

# Run tests with specific features
cargo test --no-default-features --features sync-native-tls
```

### Running Examples

```bash
# Run examples (requires AWS credentials)
cargo run --example tokio
cargo run --example async-std --no-default-features --features async-std-native-tls
cargo run --example sync --no-default-features --features sync-native-tls
cargo run --example minio
cargo run --example r2
cargo run --example google-cloud
```

## Architecture and Key Components

### Core Structure

The main `Bucket` struct (s3/src/bucket.rs) represents an S3 bucket and provides all S3 operations. Key architectural decisions:

1. **Multiple Runtime Support**: The library uses `maybe-async` to support tokio, async-std, and sync runtimes through feature flags
2. **Backend Abstraction**: HTTP requests are abstracted through backend modules:
   - `request/tokio_backend.rs` - Tokio + reqwest
   - `request/async_std_backend.rs` - async-std + surf
   - `request/blocking.rs` - Sync implementation with attohttpc

3. **Request Signing**: AWS Signature V4 implementation in `s3/src/signing.rs`
4. **Streaming Support**: Large file operations support streaming to avoid memory issues

### Feature Flags

The library uses extensive feature flags to control dependencies:

- **default**: `tokio-native-tls` runtime with native TLS
- **sync**: Synchronous implementation without async runtime
- **blocking**: Generates `*_blocking` variants of all async methods
- **fail-on-err**: Return Result::Err for HTTP errors
- **tags**: Support for S3 object tagging operations

### Testing Approach

Tests are primarily integration tests marked with `#[ignore]` that require actual S3 credentials. They're located inline within source files using `#[cfg(test)]` modules. Run ignored tests with:

```bash
cargo test -- --ignored
```

## Important Implementation Notes

1. **Request Retries**: All requests are automatically retried once on failure. Additional retries can be configured with `bucket.set_retries()`

2. **Path vs Subdomain Style**: The library supports both path-style and subdomain-style bucket URLs. Subdomain style is default.

3. **Presigned URLs**: The library supports generating presigned URLs for GET, PUT, POST, and DELETE operations without requiring credentials at request time.

4. **Error Handling**: With `fail-on-err` feature (default), HTTP errors return `Result::Err`. Without it, errors are embedded in the response.

5. **Streaming**: Use `get_object_stream` and `put_object_stream` methods for large files to avoid loading entire content in memory.

## Code Conventions

- Use existing error types from `s3/src/error.rs`
- Follow the async/sync abstraction pattern using `maybe_async` macros
- Integration tests should be marked with `#[ignore]` if they require credentials
- All public APIs should have documentation examples
- Maintain compatibility with multiple S3-compatible services (not just AWS)