# osm-pbf — AI Agent Instructions

## Commands

```bash
# Build
cargo build --workspace --all-targets

# Test (full suite)
cargo test --workspace --all-targets --exclude benches

# Run a single test
cargo test <test_name> -- --exact

# Lint
cargo clippy --workspace --all-targets

# Lint - Auto-Fix
cargo clippy --workspace --all-targets --fix --allow-dirty

# Auto-format
cargo fmt --all
```

## Architecture

Two-crate workspace:

- **`proto/`** (`osm-pbf-proto`): Protobuf-generated Rust types for the OSM PBF wire format.
- **`reader/`** (`osm-pbf-reader`): PBF file reader built on top of the `osm-pbf-proto` crate.

### `osm-pbf-proto` crate breakdown

- `src/protos/` — original `.proto` files: `fileformat.proto`, `osmformat.proto`. **Never edit these** (synced from upstream).
- `build.rs` — downloads `protoc` via `protoc-prebuilt`, generates code into `$OUT_DIR/protobuf_generated/`. **Never edit generated files.**
- `src/lib.rs` — crate root; exposes `mod protos` (generated types) and `pub mod elements` (convenience helpers).
- `src/elements.rs` — inherent `impl` blocks on generated view types plus iterator/tag-access helpers (see below).

#### Protobuf 4.x UPB kernel — generated type pattern

Every message produces a tripartite set of types:
- `Foo` — owned (heap-allocated)
- `FooView<'msg>` — immutable borrow, `Copy`; all field accessors take `self` by value and return data with lifetime `'msg` (tied to the UPB arena, **not** any local variable)
- `FooMut<'msg>` — mutable borrow

`RepeatedView<'msg, T>` implements `IntoIterator`. For scalars `View<'msg, i32> = i32`; for bytes `View<'msg, ProtoBytes> = &'msg [u8]`.  
`Optional<T>::into_option()` converts optional fields.  
`DenseNodesView::default()` returns `DenseNodesView<'static>` (useful as an empty sentinel).

#### `src/elements.rs` — public API surface

| Type / fn | Description |
|---|---|
| `Tags<'msg>` | Map-like tag access for `Node`/`Way`/`Relation` (parallel `keys`/`vals` u32 arrays) |
| `TagsIter<'msg>` | Iterator yielding `(&'msg str, &'msg str)` |
| `DenseNodeTags<'msg>` | Tag access for a single `DenseNodeRef` (slice of `keys_vals`; bounded by `kv_end`) |
| `DenseNodeTagsIter<'msg>` | Iterator over a single dense node's tags |
| `DenseNodeRef<'msg>` | Decoded dense node: `id: i64`, `lat: i64`, `lon: i64` (all delta-decoded) + `PrimitiveGroupRef`; adds `lat_deg()`, `lon_deg()`, `tags()` |
| `DenseNodeRefsIter<'msg>` | Stateful iterator over `DenseNodesView`; accumulates delta-coded id/lat/lon |
| `AnyNodeRef<'msg>` | Enum `Regular(NodeRef)` \| `Dense(DenseNodeRef)` with uniform `id()`, `lat()`, `lon()`, `lat_deg()`, `lon_deg()`; match for tags |
| `PrimitiveGroupRef<'msg>` | Group view + parent block + string table; entry point for group-level iteration |
| `AbstractRef<'msg, T>` | Generic wrapper bundling a protobuf `View` with a `PrimitiveGroupRef`; derefs to the inner view; exposes `.group()` |
| `NodeRef<'msg>` | `AbstractRef<'msg, Node>` — adds `tags()`, `lat_deg()`, `lon_deg()` |
| `WayRef<'msg>` | `AbstractRef<'msg, Way>` — adds `tags()` |
| `RelationRef<'msg>` | `AbstractRef<'msg, Relation>` — adds `tags()` |
| `ChangeSetRef<'msg>` | `AbstractRef<'msg, ChangeSet>` |
| `Element<'msg>` | Enum over all group element types: `Node(AnyNodeRef)`, `Way(WayRef)`, `Relation(RelationRef)`, `Changeset(ChangeSetRef)` |
| `ElementTypes` | `bitflags` bitset — `NODES \| WAYS \| RELATIONS \| CHANGESETS`; used with `iter_filtered_elements` |
| `decode_coordinate(raw, offset, granularity)` | Degrees = 1e-9 × (offset + granularity × raw) |

Iterator methods on `PrimitiveGroupRef<'msg>`:

| Method | Yields |
|---|---|
| `.iter_nodes()` | `NodeRef<'msg>` (regular nodes only) |
| `.iter_dense_nodes()` | `DenseNodeRef<'msg>` (dense nodes only) |
| `.iter_all_nodes()` | `AnyNodeRef<'msg>` (all nodes) |
| `.iter_ways()` | `WayRef<'msg>` |
| `.iter_relations()` | `RelationRef<'msg>` |
| `.iter_changesets()` | `ChangeSetRef<'msg>` |
| `.iter_elements()` | `Element<'msg>` (all elements, all types) |
| `.iter_filtered_elements(types)` | `Element<'msg>` (filtered; skips entire group if type not in `types`) |

Iterator methods on `PrimitiveBlockView<'msg>` (fold across all groups):

| Method | Yields |
|---|---|
| `.iter_groups()` | `PrimitiveGroupRef<'msg>` |
| `.iter_nodes()` | `NodeRef<'msg>` |
| `.iter_dense_nodes()` | `DenseNodeRef<'msg>` |
| `.iter_all_nodes()` | `AnyNodeRef<'msg>` |
| `.iter_ways()` | `WayRef<'msg>` |
| `.iter_relations()` | `RelationRef<'msg>` |
| `.iter_changesets()` | `ChangeSetRef<'msg>` |
| `.iter_elements()` | `Element<'msg>` |
| `.iter_filtered_elements(types)` | `Element<'msg>` (skips groups whose type is absent from `types`) |

#### OSM PBF encoding notes (relevant to `primitives.rs`)

- **Coordinates**: `lat`/`lon`/`id` in `DenseNodes` are delta-encoded across the sequence: each stored value is the *difference* from the previous node's value. `DenseNodeRefsIter` accumulates running sums. Regular `NodeView` fields are absolute.
- **`keys_vals` type mismatch**: The official spec declares `DenseNodes.keys_vals` as `repeated uint32`, but the `.proto` files use `int32`. **Always cast to `u32` before use** — values that appear negative as `i32` are simply large indices. `0` is the only sentinel (node delimiter); it is never a valid key or value index.
- **DenseNodes tag pattern**: `((<key_idx> <val_idx>)* 0)*` — one `0` per node even for tagless nodes. Exception: if **all** nodes in the group are tagless, `keys_vals` is omitted entirely (empty array).
- **String table**: index `0` is always the empty-string sentinel; never a valid key/value reference.
- **PrimitiveGroup constraint**: per spec, a single `PrimitiveGroup` holds only ONE element type (all nodes, or all ways, etc.), but the iterator API handles all slots regardless.

### reader/ module breakdown

Allows efficient and parallel reading of `.pbf` files through memory-mapped files or `std::io::Reader`.

**Compression**: zlib (default via `zlib-ng-compat` feature) and lzma (via `lzma` feature). Both enabled by default.

## Key Conventions

- **Error handling**: `thiserror` with `#[from]` for transparent wrapping of `io::Error`, `protobuf::Error`, `Utf8Error`. Prefer the crate-level `Result<T>` alias.
- **`.proto` files are synced from upstream** — never edit files under `proto/src/protos/`.
- **Proto code is generated** — never edit files under `$OUT_DIR`.
- **Editions**: Rust 2021, resolver v2.
