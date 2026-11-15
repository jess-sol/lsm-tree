> **About this fork.** This is [lsm-tree](https://github.com/fjall-rs/lsm-tree) for a caller
> that keeps its own log, gives every row its sequence number, and runs the tree's work on
> its own threads. A table install draws no sequence number, so a read at a seqno always
> begins on the newest tables; in return the tree keeps no older set of tables, and a read
> at an old seqno does not outlive a `clear`, a `drop_range` or a compaction filter.
> `Tree::write_sorted` writes a batch of rows that are already in key order as a sealed
> memtable of its own, read by bisection. `Tree::view` holds the tree for reads that take no
> lock, and `Config::filter_prefix_len` puts the first bytes of each key into the table
> filters, so a read under a prefix skips the tables that hold none of it. `Tiered` merges a
> level whole into the next, and `Tree::compaction` hands such a merge out in parts for the
> caller's threads to run. It also carries a fix for upstream's issue
> [#321](https://github.com/fjall-rs/lsm-tree/issues/321): the garbage collection of old
> versions keeps the one the oldest snapshot reads. The fork sits on upstream's 3.1 line
> and does not follow it: it serves until upstream's version 4 is released.

<p align="center">
  <img src="/logo.png" height="160">
</p>

[![CI](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml/badge.svg)](https://github.com/fjall-rs/lsm-tree/actions/workflows/test.yml)
[![docs.rs](https://img.shields.io/docsrs/lsm-tree?color=green)](https://docs.rs/lsm-tree)
[![Crates.io](https://img.shields.io/crates/v/lsm-tree?color=blue)](https://crates.io/crates/lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.90.0-blue)
[![dependency status](https://deps.rs/repo/github/fjall-rs/lsm-tree/status.svg)](https://deps.rs/repo/github/fjall-rs/lsm-tree)

A K.I.S.S. implementation of log-structured merge trees (LSM-trees/LSMTs) in Rust.

> [!NOTE]
> This crate only provides a primitive LSM-tree, not a full storage engine.
> For example, it does not ship with a write-ahead log.
> You probably want to use https://github.com/fjall-rs/fjall instead.

## About

This is the most feature-rich LSM-tree implementation in Rust! It features:

- Thread-safe `BTreeMap`-like API
- Mostly [safe](./UNSAFE.md) & 100% stable Rust
- Block-based tables with compression support & prefix truncation
  - Optional block hash indexes in data blocks for faster point lookups [[3]](#footnotes)
  - Per-level filter/index block pinning configuration
- Range & prefix searching with forward and reverse iteration
- Block caching to keep hot data in memory
- File descriptor caching with upper bound to reduce `fopen` syscalls
- *AMQ* filters (currently Bloom filters) to improve point lookup performance
- Multi-versioning of KVs, enabling snapshot reads
- Optionally partitioned block index & filters for better cache efficiency [[1]](#footnotes)
- Leveled and FIFO compaction
- Optional key-value separation for large value workloads [[2]](#footnotes), with automatic garbage collection
- Single deletion tombstones ("weak" deletion)
- Optional compaction filters to run custom logic during compactions

Keys are limited to 65536 bytes, values are limited to 2^32 bytes.
As is normal with any kind of storage engine, larger keys and values have a bigger performance impact.

## Sponsors

<a href="https://sqlsync.dev">
  <picture>
    <source width="240" alt="Orbitinghail" media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail.svg" />
    <source width="240" alt="Orbitinghail" media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail_dark.svg" />
    <img align="center" width="240" alt="Orbitinghail" src="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/d22fcb1e6966ce08327ea3bf6cf2ea86a840b071/public/logos/orbitinghail_dark.svg" />
  </picture>
</a>

<a href="https://svix.com">
  <picture>
    <source width="240" alt="Svix" media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/refs/heads/main/public/logos/svix.svg" />
    <source width="240" alt="Svix" media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/refs/heads/main/public/logos/svix_dark.svg" />
    <img hspace=20 align="center" width="240" alt="Svix" src="https://raw.githubusercontent.com/fjall-rs/fjall-rs.github.io/refs/heads/main/public/logos/svix_dark.svg" />
  </picture>
</a>

## Feature flags

### lz4

Allows using `LZ4` compression, powered by [`lz4_flex`](https://github.com/PSeitz/lz4_flex).

*Disabled by default.*

### bytes_1

Uses [`bytes`](https://github.com/tokio-rs/bytes) 1.x as the underlying `Slice` type.

*Disabled by default.*

## Run unit benchmarks

```bash
cargo bench --features lz4
```

## License

All source code is licensed under MIT OR Apache-2.0.

All contributions are to be licensed as MIT OR Apache-2.0.

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
