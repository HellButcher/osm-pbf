# `osm-pbf-proto` and `osm-pbf-reader`


[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](#license)
[![Rust CI](https://github.com/HellButcher/osm-pbf/actions/workflows/rust.yml/badge.svg)](https://github.com/HellButcher/osm-pbf/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/osm-pbf-reader.svg?label=osm-pbf-reader)](https://crates.io/crates/osm-pbf-reader)
[![docs.rs](https://docs.rs/osm-pbf-reader/badge.svg)](https://docs.rs/osm-pbf-reader/)

Fast OpenStreetMap PBF-File reader.

## Features

WIP ⚠

* Fast & Simple to use
* Parallelizable with `rayon` using [`par_bridge`].
* Supports zlib & lzma compressed blobs
* Zero-copy streaming `Blob` parser: the compressed payload is read directly
  from the stream into a pooled buffer — no intermediate copy through the
  protobuf runtime arena. Buffers are returned to a shared [`BufPool`] after
  decoding and reused across blobs.

[`rayon`]: https://github.com/rayon-rs/rayon
[`par_bridge`]: https://docs.rs/rayon/1.5.1/rayon/iter/trait.ParallelBridge.html#tymethod.par_bridge
[`BufPool`]: https://docs.rs/osm-pbf-reader/latest/osm_pbf_reader/buf_pool/struct.BufPool.html

## License

[license]: #license

This repository is licensed under

* MIT license ([LICENSE-MIT] or <http://opensource.org/licenses/MIT>)

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, shall be licensed as above, without any
additional terms or conditions.

[LICENSE-MIT]: LICENSE-MIT
