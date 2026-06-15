# Phase F.2 — ColumnarProjection materialization design (2026-06-15)

Direct design (no consensus run — the columnar layout pattern is
well-established across DuckDB, ClickHouse, and Vertica; the choices
specific to OpenDB are clear once the F.1 trait shape is fixed).

## What

A second `Projection` impl (per the F.1 trait decision) that
materializes table state in a **column-oriented** layout: one
`Vec<T>` per column, with null bitmaps and dictionary encoding for
text. Consumes the same WAL stream as `RowProjection`.

Selected per table via `CREATE TABLE t (...) WITH (engine = 'columnar')`
(F.1 decision). Default remains row-store; columnar is opt-in.

## Storage shape

```rust
// crates/opendb-storage/src/columnar_projection.rs (new)

pub struct ColumnarProjection {
    tables: BTreeMap<TableName, ColumnarTable>,
}

pub struct ColumnarTable {
    schema: TableSchema,           // shared with RowProjection
    row_count: usize,
    primary_key_index: HashMap<KeyString, usize>,  // PK -> row offset
    columns: Vec<Column>,
    deleted_mask: Bitmap,          // soft-delete bitmap for tombstones
}

pub enum Column {
    Int64(Vec<i64>, Bitmap),                    // values + null bitmap
    Float64(Vec<f64>, Bitmap),
    Bool(BitVec, Bitmap),                       // packed bools + null bitmap
    Timestamp(Vec<i64>, Bitmap),                // microseconds since epoch
    Text(TextColumn),                           // see below
    Json(Vec<Vec<u8>>, Bitmap),                 // raw JSON bytes (Phase F.2 MVP);
                                                // Phase F.3 may add JSON path index
}

pub enum TextColumn {
    Plain(Vec<String>, Bitmap),                 // for unique/high-cardinality
    Dict {                                      // for low-cardinality (<= 32k uniques)
        codes: Vec<u32>,
        dictionary: Vec<String>,                // codes index into this
        nulls: Bitmap,
    },
}
```

The `Bitmap` is `roaring::RoaringBitmap` (battle-tested, compact for
sparse) or a simple `Vec<u64>` for dense bitmaps. Phase F.2 MVP uses
the simpler `BitVec` from `bitvec` crate; switch to roaring if memory
profiling shows it matters.

## Dictionary encoding decision

Apply automatic dict encoding when:
- Column type is Text.
- At INSERT time, the running unique-value count is ≤ 32K (configurable).
- If a later insert pushes uniques past the threshold, the column
  transparently promotes to `Plain` (one-time decode + rewrite during
  the next vacuum / merge cycle, or eagerly if the table is small).

The dict promotion path is the only "transition" piece; it's bounded
and runs during apply (single-writer, no races).

## Row addressing — primary key + row offset

`primary_key_index: HashMap<KeyString, usize>` maps the PK string to
the row's offset in the columns. INSERT appends to columns + indexes
the new offset. DELETE flips the bit in `deleted_mask` (no array
shrink — kept tombstoned until vacuum).

This means **point lookups** by PK on a columnar table go through the
HashMap (O(1) avg). Scans iterate over the columns directly, skipping
indices where `deleted_mask` is set.

Phase F.2 explicitly does NOT optimize point lookups on columnar
tables — that's what RowProjection is for. The `Projection::lookup_row`
impl on ColumnarProjection does its O(1) PK index lookup, then
reassembles the row by walking each column at that offset.
Slower than RowProjection but correct.

## UPDATE handling

PG-style: UPDATE = INSERT-new + bump deleted_mask on old (versioned via
Phase C MVCC's xmin/xmax). Pre-C: UPDATE rewrites in place at the
existing offset and re-indexes (simpler, breaks scan-while-updating
semantics — acceptable because Phase F.2 alone is read-mostly and
phase C must land before columnar tables go into mixed OLTP use).

Documented as a known limitation in Phase F.2's release notes:
"Columnar tables before Phase C land are READ COMMITTED only and
scan-while-updating may see torn rows."

## WAL replay

`ColumnarProjection::apply(record: &CommitRecord)` translates each
`Mutation::InsertRow / UpdateRow / DeleteRow` into the columnar
append / mark-delete / promote path. CreateTable initializes the
columns to empty `Vec<T>` of the right type. AlterTable adds/removes
columns (Vec push/pop or reorder).

The WAL stream is identical to row-store — both projections consume
the same `Mutation` enum. The choice is purely a materialization
choice.

## Memory layout consequences

For a 100-column table with 1M rows:
- RowProjection: 1M `BTreeMap<String, Value>`, ~140 MB at typical
  string/value sizes.
- ColumnarProjection: 100 `Vec<T>` of 1M entries, ~80 MB raw + dict
  savings on text columns. Often 5-10× smaller end-to-end with H
  compression.

Scan performance per F.1's analytical doc projections:
~5-10× faster on multi-column aggregations, sometimes more after
G + I.

## Acceptance criteria

- `CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, age INT) WITH
  (engine = 'columnar')` creates a `ColumnarTable` in
  `ColumnarProjection.tables`.
- `INSERT INTO t VALUES (1, 'Ada', 30)` appends to each column at
  offset 0, registers `'1' -> 0` in `primary_key_index`.
- `SELECT name FROM t WHERE age > 25` scans the age column,
  produces a filter mask, applies to the name column, returns
  rows. Per Phase G this becomes vectorized.
- `SELECT * FROM t WHERE id = 1` reassembles the row at offset 0.
- A dict-encoded text column with 5 distinct values across 100K rows
  uses ≤ 500 KB (codes + 5-entry dict) vs ~3 MB plain.
- Storage tests: extend the existing 103 with parallel-shape tests
  for ColumnarProjection. Goal: 30+ new tests covering create,
  insert, update (pre-C in-place), delete-tombstone, dict-promotion,
  scan, lookup-by-PK.

## Effort

**5-7 weeks.** Breakdown:
- ColumnarTable + Column variants + bitmaps: 1 wk
- WAL replay path (each Mutation maps to columnar op): 1 wk
- Dict encoding + promotion: 1 wk
- Per-Mutation tests (parity with RowProjection where applicable): 1 wk
- F.1 trait impl + dispatch wiring: 1 wk
- Bench (TPC-H Q1 over 1M rows, single-threaded baseline): 0.5 wk
- Buffer for the unknowns: 0.5-1 wk

## Out of scope for Phase F.2

- **Vectorized scan kernels.** That's Phase G.
- **Compression codecs.** That's Phase H. Phase F.2 stores raw `Vec<T>`;
  compression layers on top via codec-aware `Vec` slices in Phase H.
- **Parallel scan.** That's Phase I.
- **Cost-based planner.** Predicate routing in Phase F.2 is naïve —
  every column read is sequential. Optimal column ordering, prune-by-
  zonemap, late materialization — all later.
- **JSON path indexing** (the `Vec<Vec<u8>>` for JSON columns is just
  byte storage; querying by JSON path is a Phase J+ feature).

## Dependencies

- **F.1** Projection trait — must land first (Phase F.2 is a trait
  impl).

## Track items

WP6 `F.2 — Per-column Vec<T> storage` slot. Already in track.
