# kafcli

A command-line client for Apache Kafka, written in Rust.

**Status: pre-alpha.** Interfaces and behavior may change without notice.

## Building from source

kafcli links librdkafka, OpenSSL and curl statically; they are compiled from source during the build. Install these build prerequisites:

- Rust 1.99 (pinned by `rust-toolchain.toml`)
- `cmake`
- A C compiler and a C++ compiler (for example gcc or clang; on macOS, the Xcode command line tools)
- `perl`, used by OpenSSL's `Configure` script

Then run `cargo build --release -p kafcli`. The release binaries that CI publishes need none of these tools to run.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
