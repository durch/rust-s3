# Test and CI coverage

`make` and `make ci` run the full credential-free local suite: format check,
clippy, all nine S3 runtime/TLS library and example configurations, representative
API doctests, and the `aws-region` and `aws-creds` test suites. Ignored provider
tests are opt-in through `make integration-test` or `make test-ignored` from
`s3/`. `make test-all` and `make ci-all` include those integrations and require
the corresponding provider or credential setup.

The S3 feature matrix uses `cargo test --tests --examples` so each configuration
checks the library tests and compiles its examples. TLS implementations are
covered separately from doctests because the TLS choice does not change the
generated public API. Representative doctest commands also keep coverage for
the default `tags` feature and for the `blocking` API:

| Runtime | TLS configuration | Feature selection | Coverage |
| --- | --- | --- | --- |
| Tokio | native TLS | default features | library tests and examples; default feature set includes `tags` |
| Tokio | no TLS | `with-tokio`, `aws-creds/http-credentials` | library tests and examples |
| Tokio | rustls | `with-tokio`, `tokio-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| async-std | base/no TLS | `with-async-std-hyper`, `aws-creds/http-credentials` | library tests and examples |
| async-std | native TLS | `async-std-native-tls`, `aws-creds/http-credentials` | library tests and examples |
| async-std | rustls | `async-std-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | native TLS | `sync`, `sync-native-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | rustls | `sync`, `sync-rustls-tls`, `aws-creds/http-credentials` | library tests and examples |
| sync | no TLS | `sync`, `aws-creds/http-credentials` | library tests and examples |

Doctests run for default Tokio, Tokio with `blocking`, async-std native TLS with
`tags` and `blocking`, and sync native TLS with `tags`. The sync documentation
examples are compiled in that representative configuration; the other sync TLS
variants still run their library and example coverage.

The GitHub workflow runs formatting and support-crate checks once, then runs
three bounded S3 jobs for Tokio, async-std, and sync. Each runtime job covers all
three TLS configurations. Cargo registry and build caches are scoped by runtime,
resolved workspace manifest, and Rust compiler fingerprint; cache paths exclude
Cargo configuration and credentials. The repository ignores `Cargo.lock`, so
CI resolves dependencies at run time and does not promise a fixed dependency
snapshot across workflow runs.
