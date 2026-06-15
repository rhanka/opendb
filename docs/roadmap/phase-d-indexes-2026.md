# Phase D — secondary B-tree + hash indexes design (2026-06-15)

Direct design from the existing roadmap WP4 outline. No consensus run —
the design space follows PG's pattern closely and is not contested.

## What

Beyond the current `BTreeMap<key, Row>` primary-key lookup, add
secondary indexes: B-tree (default, range-capable) and hash
(equality-only, smaller). Indexes are catalog entities, parser-
visible, and consulted by the executor when query predicates match.

## Trait + storage

```rust
// crates/opendb-storage/src/index.rs (new)
pub trait Index: Send + Sync {
    fn name(&self) -> &str;
    fn table(&self) -> &str;
    fn columns(&self) -> &[ColumnName];
    fn lookup_eq(&self, key: &CompositeKey) -> Box<dyn Iterator<Item = RowKey> + '_>;
    fn lookup_range(&self, lo: &CompositeKey, hi: &CompositeKey) -> Box<dyn Iterator<Item = RowKey> + '_>;
    fn insert(&mut self, key: CompositeKey, row_key: RowKey);
    fn delete(&mut self, key: &CompositeKey, row_key: &RowKey);
}

pub struct SecondaryBTreeIndex {
    name: String,
    table: String,
    columns: Vec<ColumnName>,
    map: BTreeMap<CompositeKey, BTreeSet<RowKey>>,  // one row_key per
                                                    // unique composite;
                                                    // BTreeSet for
                                                    // multi-row matches
}

pub struct SecondaryHashIndex {
    name: String,
    table: String,
    columns: Vec<ColumnName>,
    map: HashMap<CompositeKey, BTreeSet<RowKey>>,
}
```

`CompositeKey = Vec<Value>` (lexicographic for B-tree, hash-stable for
hash; serializes the index's columns in order).

Indexes are owned by `RowProjection::Table` alongside the existing
`indexes: Vec<IndexDescriptor>` field — extend that to also hold the
materialized `Box<dyn Index>` (or, for Option P consistency, `enum
IndexImpl { BTree(SecondaryBTreeIndex), Hash(SecondaryHashIndex) }`).

## Parser surface

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name>
    ON <table> [USING { btree | hash }] (<col>[, <col>...]);

DROP INDEX [IF EXISTS] <name>[, <name>...];
```

- Default `USING btree` if omitted.
- `UNIQUE INDEX` enforces uniqueness on INSERT/UPDATE.
- Multi-column = composite key (lexicographic order for B-tree).

## Mutation + WAL

Two new `Mutation` variants (extending the existing `IndexDescriptor`
which is already in the WAL schema for ALTER TABLE ADD INDEX):

```rust
pub enum Mutation {
    // ... existing ...
    CreateSecondaryIndex { name: String, table: String, columns: Vec<ColumnName>, kind: IndexKind, unique: bool },
    DropSecondaryIndex { name: String },
}

pub enum IndexKind { BTree, Hash }
```

`RowProjection::apply_inner` builds the index from the table's current
rows by walking `table.rows.iter()` and indexing each row. DROP removes
the index entry. INSERT/UPDATE/DELETE on the table also maintain the
indexes — handled inside the existing `apply_insert_row` /
`apply_delete_row` / `apply_update_row` paths.

## Executor: predicate routing

In `crates/opendb-sql/src/executor.rs`, the existing predicate
matching paths (SELECT WHERE, UPDATE WHERE, DELETE WHERE) currently
full-scan via `table.rows.iter()`. Phase D inserts a planning step:

1. Decompose the WHERE clause into per-column predicates.
2. Find indexes whose columns match a prefix of the predicate set
   (equality predicates only for hash; equality OR range for B-tree).
3. If a useful index exists, replace the full scan with `index.lookup_*()`
   and then re-check remaining predicates per returned row.
4. If no useful index, fall back to the existing full-scan path.

Statistics are NOT required for Phase D — the choice is deterministic:
exact-prefix-match index wins; otherwise scan. (Cost-based planning
is Phase F.3+.)

## Acceptance criteria

- `CREATE INDEX idx_a ON pgbench_accounts (aid)` + `SELECT * FROM
  pgbench_accounts WHERE aid = 42` returns in O(log N), measured via
  `OPENDB_PERF_TIMING=1` showing a new `executor.scan.indexed` span
  that doesn't grow with table size.
- HammerDB NewOrder transaction's `customer_id` lookup uses the
  customer secondary index (no full scan of the customer table on
  every NewOrder).
- DROP INDEX cleanly unwires the executor's index choice; subsequent
  queries fall back to full scan without error.
- UNIQUE INDEX rejects duplicate inserts with the same composite key.
- Tests: existing 103 storage tests stay green + new tests for each
  of {create, drop, lookup_eq, lookup_range, unique-violation,
  multi-col-composite}.

## Effort

**2-3 weeks.** Breakdown:
- Index trait + 2 impls: 3 days
- Parser + Mutation + WAL replay: 2 days
- Executor planning step (predicate → index match): 4 days
- Tests + golden: 2 days
- Bench (HammerDB NewOrder scan-elimination): 2 days

## Out of scope (Phase D follow-ups)

- **Covering indexes** (`INCLUDE (extra_cols)` syntax). Useful for
  index-only scans; add after Phase D ships.
- **Expression indexes** (`CREATE INDEX ON t (lower(name))`).
  Requires expression evaluation in the index path; defer.
- **Partial indexes** (`WHERE` clause on CREATE INDEX). Same.
- **GIN / BRIN / GiST / SP-GiST**. Specialized index types; only B-tree
  + hash are required for the OLTP acceptance demos.
- **Cost-based optimizer**. Phase D uses deterministic exact-match
  index selection. Cost-based planning is Phase F.3+ once we have
  multiple alternative orderings.

## Track items

Already specced at the WP level in track (D.1-D.5). The breakdown
above slots cleanly into those 5 items.
