# Phase E.3 — WAL binary framing decision (2026-06-11)

5-way agent consensus on the replacement codec for the JSON WAL frames
(`serde_json::to_vec(record)` today).

**User ratified Option X (postcard via WalCodec trait) on 2026-06-11.**

## Decision: Option X — serde-compatible binary codec, **postcard** preferred

5/5 votes for X. Estimated effort: **1 week**. Single dominant risk
across voters: **bincode 2.0 semver churn forces a one-time WAL format
migration before GA if we pin 1.x today**. Mitigation: wrap the codec
behind a small `WalCodec` trait so swapping is a one-file change.

Recommended codec: **postcard** (stable, varint-compact, no-std-friendly,
well-tested in embedded Rust). Bincode is acceptable but its 2.0 churn
is the named risk.

## Consensus rationale

1. **Proportionality.** JSON encode/decode is sub-1 % of seed cost today
   (per `OPENDB_PERF_TIMING=1` runs from Phase E.1/E.2 benches). Spending
   300-500 LOC on a hand-rolled zero-copy format chases a non-bottleneck.
   The 3-5× size reduction and ~10× decode speed of *any* binary codec
   captures the real wins.

2. **Schema evolution stays free with serde derives.** Every new
   `Mutation` variant in `commit_stream.rs` costs zero encoder/decoder
   code with serde. A hand-rolled format taxes every new variant with
   an encoder + decoder + golden test pair — that's a permanent
   maintenance surface on a non-hot path.

3. **Framing header stays untouched.** The `ODW1` magic + version +
   payload-length + CRC32 header from today's WAL is independent of the
   payload codec. Torn-tail detection logic doesn't change. The
   `durable_prefix_len` cache from 2026-05-20 keeps working.

4. **Forensics tooling preserved.** Python / Rust tooling that decodes
   WAL files for incident response stays trivial with a public codec.
   A hand-rolled format would require shipping a separate decoder.

5. **Optionality preserved via `WalCodec` trait.** If profiling ever
   shows the codec IS the bottleneck (it isn't today), a hand-rolled
   Option Y can land later as a second `WalCodec` impl. The trait seam
   localizes the swap.

## Acceptance criteria

- New `WalCodec` trait in `crates/opendb-storage/src/wal_codec.rs` with
  `encode(record: &CommitRecord) -> Vec<u8>` and
  `decode(bytes: &[u8]) -> OpenDbResult<CommitRecord>`.
- Default impl: `PostcardCodec` (fallback to `BincodeCodec` only if
  postcard licensing or feature gap blocks).
- Wire `Wal::append*` and `decode_records` to call through the trait.
- New WAL files use the new codec; readers detect old JSON frames via
  the `WAL_FRAME_VERSION` header field (bump to 2 for binary) and decode
  both for one milestone before deprecating v1.
- Bench: re-run the sentropic POC seed; expect the JSON encode span
  (currently ~7-15 µs/call per `wal.encode_frame_serde_json`) to drop
  by ~10×. WAL file size on the 500-row POC should drop 3-5×.

## Out of scope

- Zero-copy decode for column values (would be Option Y; revisit only
  after Phase C MVCC reshapes the hot path).
- Cross-language schema definition (Protocol Buffers / FlatBuffers).
  Not justified at this size; serde + postcard handles Rust↔Rust cleanly.
- Compression *of* WAL frames (separate concern; `zstd` framing can
  layer on top of the binary codec later).

## Provenance

5 independent voter outputs, all converging on Option X with 1-week
effort and bincode 2.0 semver as the named top risk. Three independently
suggested postcard behind a `WalCodec` trait. Voter transcripts in
`/tmp/claude-0/.../tasks/a82ad728*, ac6e78ae*, ab91087df*, a6fd15100*,
aa4e2e5a5*`. User ratification recorded in
`docs/roadmap/decisions-for-user-2026-06-11.md`.
