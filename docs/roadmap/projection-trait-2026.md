# Phase F.1 — Projection trait design decision (2026-06-11)

5-way agent consensus on how to expose two projection layouts (existing
row-store + future columnar) over the same WAL.

**User ratified Option P (static trait, per-table opt-in) on 2026-06-11.**

## Decision: Option P — static trait, two implementations, per-table opt-in

5/5 votes for P. Estimated effort: **4-6 weeks** (median 6; one voter at
4). Top risk across voters: **cross-engine joins between Row and
Columnar tables need a unifying iterator that may leak storage details
into the executor**.

## Concrete shape

```rust
pub trait Projection {
    fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()>;
    fn scan_table(
        &self,
        table: &str,
        cols: &[String],
        predicates: &[Predicate],
    ) -> ScanIter<'_>;
    fn lookup_row(&self, table: &str, key: &str) -> Option<Row>;
}

pub struct RowProjection { /* existing impl, unchanged */ }
pub struct ColumnarProjection { /* new in Phase F */ }
```

Per-table choice via `CREATE TABLE t (...) WITH (engine = 'columnar')`.
Engine.prepare dispatches to the right projection from a `TableEngine`
column added to the catalog. Default = `row` (OLTP), keeps every
existing migration working.

## Consensus rationale

1. **Reversibility.** WAL is the source of truth; a table created as
   row-store can be re-materialized as columnar later (and vice versa)
   by replaying the WAL into the other projection. Option P is
   reversible per-table; Option Q's coherence bugs aren't.

2. **Pay-for-what-you-need.** Option Q's dual-write doubles apply
   latency on the WAL task **forever**, to serve scans on tables that
   may never be scanned analytically.

3. **Coherence-bug class is permanent if shared.** Two projections of
   the same row that drift from the same WAL (validate paths, default
   resolution, FK propagation) are silent correctness failures. The
   trait boundary forecloses that class entirely.

4. **Pre-HTAP discipline pays off later.** A layout-agnostic executor
   (what the trait forces) is the same discipline a future HTAP layer
   would need anyway. Skipping it now would force a rewrite later.

5. **Option P does not foreclose Option Q.** If a workload later proves
   it needs per-table HTAP, a third `DualProjection` variant can land
   behind the same trait without touching `RowProjection` or
   `ColumnarProjection`.

## Acceptance criteria

- `Projection` trait in `crates/opendb-storage/src/projection.rs` with
  the signatures above (and any minimal extensions needed for indexes
  / constraints — they should be storage-internal).
- `RowProjection` refactored to implement the trait (no behavior
  change; the existing 100+ storage tests stay green).
- `ColumnarProjection` MVP per WP6.F.2 (`Vec<i64>`, `Vec<f64>`,
  dict-encoded text, null bitmap) implements the trait, opt-in via
  catalog flag.
- `SqlEngine` dispatches per-table at prepare time; cross-engine joins
  use a unified `ScanIter` even when sides differ.
- Acceptance bench: TPC-H Q1-shape over 1M lineitem rows with table
  declared `WITH (engine = 'columnar')` — cold ≤ 250 ms, warm ≤ 120 ms.
- Row-store regression: sentropic POC seed within ±10 % of pre-trait
  baseline (the trait dispatch must be a static enum match, not
  dynamic dispatch — see implementation note).

## Implementation note (avoid trait-object overhead)

Use a Rust `enum ProjectionRef<'a> { Row(&'a RowProjection), Columnar(&'a ColumnarProjection) }`
instead of `&dyn Projection`. Static dispatch keeps the OLTP path's hot
loop free of vtable indirection. The trait exists to enforce the API
contract, not to be the runtime dispatch mechanism.

## Out of scope for Phase F.1

- HTAP / dual-write per table (Option Q). Add as a third trait impl
  later if demand proves real.
- Materialized view / `ProjectionsMergeTree`-style alternative orderings
  (ClickHouse-style). Belongs in Phase F.3+ once the trait is proven.
- Cross-engine FK constraints. Phase F.1 assumes FKs stay within the
  row projection; later phases revisit when columnar tables need to
  reference row tables.

## Provenance

5 independent voter outputs converging on Option P. Four voters
estimated 6 weeks, one 4. Top-risk phrasing converged identically
across four voters ("cross-engine joins need a unifying iterator");
the fifth named the related "trait abstraction leaks row-shaped
assumptions". Voter transcripts in `/tmp/claude-0/.../tasks/aa3ed671*,
a6cb63ed7*, a41d0bfa4*, aa533f167*, a42caaa70*`. User ratification
recorded in `docs/roadmap/decisions-for-user-2026-06-11.md`.
