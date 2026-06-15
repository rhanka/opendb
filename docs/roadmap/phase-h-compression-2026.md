# Phase H — per-column compression codecs design (2026-06-15)

5-way Opus consensus on the default codec stack for OpenDB's columnar
Phase F.2 projection.

## Decision: Option A — LZ4 + ZSTD with probe-based auto-selection

4/4 successful Opus voters chose Option A independently (one vote was
rate-limited; no dissents). Estimated effort: **3 weeks**. Top risk
converged across voters: **ZSTD-only on `l_shipdate` misses Delta's
2-5× ratio, risking the < 100 MB L3 budget on wider future schemas**.

## What

Two codecs, picked per column at write time by a small probe; Dict
already exists at the F.2 layer for text. Specialty codecs (Delta,
DoubleDelta, T64, FPC, Gorilla) are explicitly deferred to a Phase
H.2 sub-phase if a real workload demands them.

```rust
// crates/opendb-storage/src/codec.rs (new)

pub trait ColumnCodec: Send + Sync {
    fn encode(&self, raw: &[u8]) -> Vec<u8>;
    fn decode(&self, compressed: &[u8], decoded_buf: &mut Vec<u8>) -> OpenDbResult<()>;
    fn name(&self) -> &'static str;          // "lz4" | "zstd-level-N" | "none"
}

pub struct Lz4Codec;
pub struct ZstdCodec { pub level: i32 }      // 1-22; default 3

pub fn select_codec_for_column(probe_bytes: &[u8]) -> Box<dyn ColumnCodec> {
    // ~64 KB probe. Compress with LZ4 and ZSTD level 3 in parallel,
    // pick the one with the better (compressed_size, decode_speed)
    // pareto-optimal point under a 50 % budget rule:
    //   - if ZSTD ratio is ≥ 1.25× LZ4 ratio, pick ZSTD (denser);
    //   - else pick LZ4 (faster decode wins ties).
    // The constant 1.25 is tunable; ClickHouse uses similar heuristics.
    todo!()
}
```

## Storage layout per chunk

Each `ChunkColumn` (Phase G's 2048-row block) is compressed
independently. Compression is at chunk granularity, not column-wide
— so:

- Decode happens **on demand** at chunk read time into a thread-local
  decode buffer (`thread_local!` or per-operator).
- The Phase G kernels still operate on `&[T]` slices (uncompressed).
  Phase H adds a decompress step at the start of each chunk's
  consumption, NOT inside the kernel hot loop.
- For predicate-pushed-down filters that prune entire chunks via
  zonemap (Phase H.future), the chunk is never decoded.

```rust
// ChunkColumn becomes a thin wrapper that lazily decodes:
pub enum CompressedChunkColumn {
    Int64 { compressed: Vec<u8>, codec: Box<dyn ColumnCodec>, n_rows: usize },
    // ... per type ...
}

impl CompressedChunkColumn {
    pub fn decode_into<T>(&self, buf: &mut Vec<T>) -> OpenDbResult<&[T]> { /* ... */ }
}
```

## Phase G integration — no regression

The Phase G operators stay shape-compatible: `ChunkOperator::next_chunk`
returns a `Chunk` of decoded `ChunkColumn`s. The compression layer
sits between the columnar projection's `Vec<u8>` (compressed) and the
operator's `ChunkColumn` (decoded). A `Chunk` carries either decoded
slices OR a deferred decompress closure depending on whether downstream
operators read the column.

For tables created WITHOUT compression (operator can disable via
`CREATE TABLE t (...) WITH (engine = 'columnar', compression = 'none')`),
the codec is a no-op and `ChunkColumn` is a zero-copy slice into the
underlying `ColumnarProjection::Column`'s `Vec<T>` — identical to
Phase F.2 / G's perf path. **No regression on the uncompressed path.**

## Acceptance criteria

- TPC-H lineitem (1 M rows, 170 MB raw) compresses to < 60 MB total
  with the default LZ4+ZSTD probe.
- TPC-H Q1 over the compressed table: ≤ 150 ms warm (vs Phase G's
  uncompressed ~120 ms — the ~30 ms delta is the decompression cost,
  acceptable).
- `CREATE TABLE t WITH (engine='columnar', compression='none')`
  shows the SAME Q1 latency as Phase G measured pre-Phase-H (within
  noise band).
- Sweep doc: `docs/bench/column-compression-<DATE>.md` with per-column
  compressed sizes + per-codec decode throughput on the lineitem
  columns.
- Operator override accepted via `CREATE TABLE t (... col TEXT
  CODEC(lz4)) WITH (engine='columnar')`. Default if codec is omitted:
  auto-probe.

## Effort

**3 weeks.** Breakdown:
- `ColumnCodec` trait + LZ4 impl + ZSTD impl: 0.5 wk (both crates are
  drop-in via `lz4_flex` and `zstd`).
- Per-chunk encode + decode at the F.2 boundary: 1 wk.
- Probe-based selector + small tuning experiments: 0.5 wk.
- Parser: `CREATE TABLE ... CODEC(...)` syntax + per-column override
  in catalog: 0.5 wk.
- Tests + sweep bench: 0.5 wk.

## Out of scope for Phase H MVP

- **Delta, DoubleDelta, T64, FPC, Gorilla, Dict-int** (specialty
  codecs from ClickHouse's lineup). All voters flagged Delta on
  timestamps as the biggest missed win. **Phase H.2** can add them as
  drop-in `ColumnCodec` impls once a workload demands the ratio bump.
- **Zonemap / min-max sketches per chunk** for predicate pruning.
  Phase H ships compression-at-rest; pruning-during-scan is Phase
  H.3 (cheap to add atop the chunk boundaries we'll already have).
- **Page-level compression** (multiple chunks bundled). Marginal gain;
  per-chunk compression keeps decompression scoped to the chunks
  Phase G operators actually consume.
- **Cross-column shared dictionaries.** Each text column has its own
  dict from F.2. A future global string-pool for joins-heavy
  workloads can layer on later.

## Dependencies

- **F.1 Projection trait** (`Projection::scan_table` returns chunks
  the compression layer decodes).
- **F.2 ColumnarProjection** (the source of `Vec<u8>` raw column data
  the codecs compress).
- **G Vectorized exec** is **optional** but co-beneficial: G's chunk
  boundaries are exactly the compression boundaries. Phase H without
  Phase G is technically possible (full-column codec) but loses the
  chunk-locality benefit.

## Provenance

5 voters launched (all Claude Opus); 1 vote rate-limited and returned
no decision (no impact — the remaining 4 were unanimous A). Voter
transcripts in `/scratch/tmp/claude-0/.../tasks/a080d899* (rate-
limited), ae66408ce*, a5763e696*, aea3a6f5c*, a8ade819d*`. All
successful voters chose 3 weeks and named missed Delta wins on
timestamps as the top risk.

## Track item

WP6 `H — per-column compression codecs` slot. Already in track.
