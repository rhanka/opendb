# Phase G — vectorized chunk execution design (2026-06-15)

Direct design. Vectorized / chunk-based execution is the canonical
OLAP query-exec pattern (Volcano-with-chunks; see DuckDB Vector +
DataChunk, ClickHouse Block, Apache Arrow RecordBatch). No
architectural contest.

## What

Replace the current per-row dispatch in the executor's hot paths
(scan, filter, aggregate, project) with **chunk-based** dispatch that
processes 2048 rows at a time. Each operator consumes chunks and
produces chunks. Per-row work happens inside tight loops over typed
slices that the compiler auto-vectorizes (SIMD).

Reference: DuckDB's "Vector" + "DataChunk" abstraction:
<https://duckdb.org/docs/internals/vector.html>

## Chunk shape

```rust
// crates/opendb-sql/src/chunk.rs (new)

pub const CHUNK_ROWS: usize = 2048;

pub struct Chunk {
    pub n_rows: usize,                    // active rows in this chunk (<= CHUNK_ROWS)
    pub columns: Vec<ChunkColumn>,        // schema-aligned columns
}

pub enum ChunkColumn {
    Int64(Box<[i64; CHUNK_ROWS]>, Bitmap),
    Float64(Box<[f64; CHUNK_ROWS]>, Bitmap),
    Bool(Box<[u64; CHUNK_ROWS / 64]>, Bitmap),
    Timestamp(Box<[i64; CHUNK_ROWS]>, Bitmap),
    Text(TextChunk),
    Json(Vec<Vec<u8>>, Bitmap),           // can't easily fit fixed-size; box-of-vec
}

pub enum TextChunk {
    Plain(Vec<Option<String>>),           // simple; expensive for narrow strings
    Dict { codes: Box<[u32; CHUNK_ROWS]>, dictionary: Arc<Vec<String>>, nulls: Bitmap },
    Inline { offsets: Box<[u32; CHUNK_ROWS+1]>, data: Vec<u8>, nulls: Bitmap }, // contig bytes
}
```

`Bitmap` here is a packed `[u64; CHUNK_ROWS/64]` (32 × u64 for 2048
rows). Trivial to AND/OR for combining filters.

`CHUNK_ROWS = 2048` matches DuckDB's default and fits in L2 cache on
most contemporary CPUs for narrow columns.

## Operators

```rust
// crates/opendb-sql/src/exec/op.rs (new)

pub trait ChunkOperator {
    fn next_chunk(&mut self) -> OpenDbResult<Option<Chunk>>;
}

pub struct TableScan { /* iterates ColumnarProjection.columns at CHUNK_ROWS strides */ }
pub struct FilterMask { input: Box<dyn ChunkOperator>, predicate: Predicate }
pub struct HashAggregate { input: Box<dyn ChunkOperator>, grouping: Vec<ColIdx>, aggs: Vec<AggKind> }
pub struct Projection { input: Box<dyn ChunkOperator>, exprs: Vec<Expr> }
pub struct Limit { input: Box<dyn ChunkOperator>, remaining: usize }
```

Operators chain by composition. The plan tree pulls chunks from the
root downward (Volcano-style `next_chunk`). The hot loop inside each
operator is a tight typed loop:

```rust
// FilterMask::next_chunk excerpt
fn next_chunk(&mut self) -> OpenDbResult<Option<Chunk>> {
    let mut chunk = match self.input.next_chunk()? { Some(c) => c, None => return Ok(None) };
    let mask = match &chunk.columns[self.predicate.col_idx] {
        ChunkColumn::Int64(values, _) => {
            let mut m = Bitmap::new(chunk.n_rows);
            // Hot loop — the compiler auto-vectorizes this.
            for i in 0..chunk.n_rows {
                if values[i] > self.predicate.literal_i64 {
                    m.set(i);
                }
            }
            m
        }
        // ... other types ...
        _ => unimplemented!(),
    };
    chunk.apply_mask(&mask);   // shrinks n_rows + compacts the surviving rows
    Ok(Some(chunk))
}
```

## Kernels (typed inner loops, SIMD-friendly)

Each kernel takes a typed slice and outputs a typed slice/bitmap:

- `sum_i64(slice: &[i64], mask: &Bitmap) -> i64`
- `count(mask: &Bitmap) -> u64`
- `avg_f64(slice: &[f64], mask: &Bitmap) -> f64`
- `min_*` / `max_*` per type
- `eq_i64`, `lt_i64`, `gt_f64`, … producing bitmaps
- `groupby_hash(grouping_cols: &[ChunkColumn], aggregator: &mut HashTable)`

All kernels are `#[inline]` free functions. Rust's auto-vectorization
handles `sum`, `count`, `eq`, `lt` cleanly on `Vec<i64>` / `Vec<f64>`.
For `groupby_hash`, the hot loop is `hash() + insert_or_increment()`;
SIMD is harder there, but cache-friendly contiguous keys help.

Manual SIMD intrinsics: **out of scope for Phase G MVP**. Rust's
auto-vectorization on AVX2 (default for `release` build) gets us
within ~2× of hand-written intrinsics for these kernels. Revisit if
profiling shows a specific kernel as a bottleneck after H + I.

## Bridging RowProjection (OLTP path stays untouched)

`RowProjection::scan_table` (the trait method) returns an iterator
that produces chunks by walking the BTreeMap and assembling them in
batches of `CHUNK_ROWS`. Cost is dominated by the BTreeMap walk
(each row needs decoding), so RowProjection scan throughput is
unchanged — the chunk wrapping is a thin adapter, not a perf path.

`ColumnarProjection::scan_table` produces chunks **zero-copy** by
slicing its underlying `Vec<T>` columns into `&[T; CHUNK_ROWS]`
references (or near-zero-copy when the table's row count isn't a
multiple of CHUNK_ROWS — last chunk is partial).

## Acceptance criteria

- `SELECT COUNT(*) FROM t` on a 1 M-row `ColumnarProjection` table:
  ≤ 20 ms warm. (Per-chunk count via `mask.count_ones()`; ~500
  chunks × ~30 µs/chunk.)
- `SELECT SUM(amount) FROM t WHERE region = 'EU'` on the same:
  ≤ 60 ms warm.
- `SELECT region, SUM(amount) FROM t GROUP BY region` (5 regions):
  ≤ 80 ms warm.
- Auto-vectorization confirmed by `cargo asm` on `sum_i64`,
  `eq_i64`, `gt_f64` — output should include `vpaddq` / `vpcmpeqq`
  / `vcmpgtpd` (AVX2) on a modern build.
- RowProjection regression: existing 103 storage tests + sentropic
  POC seed still within ±10 % of pre-Phase-G baseline.

## Effort

**5-7 weeks.** Breakdown:
- Chunk + ChunkColumn + Bitmap: 1 wk
- ChunkOperator trait + 5 base impls (Scan, FilterMask, HashAggregate,
  Projection, Limit): 1.5 wk
- Typed kernels (sum/count/avg/min/max + eq/lt/gt/le/ge per type): 1.5 wk
- Planner: translate `Statement::Select` into a ChunkOperator chain: 1 wk
- Tests + bench: 1 wk
- Buffer: 0-1 wk

## Out of scope for Phase G

- **Hand-rolled SIMD intrinsics** (AVX2/AVX-512 explicit). Phase G
  ships auto-vectorized Rust loops; intrinsics are a Phase G.2
  follow-up if profiling shows ≥ 30 % gap to hardware peak.
- **Parallel chunk dispatch.** That's Phase I (morsels).
- **Compression-aware kernels** (e.g., decompress-while-summing).
  Phase H ships compression; chunk operators read from a decompressed
  Vec<T> for Phase G. A future "fusion" pass can combine decompression
  + kernel into a single loop.
- **Late materialization** (don't read a column until you know you
  need it). Subtle; defer to Phase G.2.

## Dependencies

- **F.1 — Projection trait**: hard prerequisite (Phase G dispatches
  per-projection at scan time).
- **F.2 — ColumnarProjection**: hard prerequisite (Phase G's perf win
  is on the columnar side; row-side gets the chunking adapter but no
  observable gain).

## Track items

WP6 `G — vectorized chunk execution` slot. Already in track.
