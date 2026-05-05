# OpenDB Milestone 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first executable OpenDB milestone: a Rust workspace with a 3-node k3s deployment shape, operator-lite CRD, root-range commit stream/WAL primitives, rebuildable row projection, minimal SQL path, and pgwire-compatible smoke endpoint.

**Architecture:** Milestone 1 uses one replicated root range/tablet as the distributed unit. The canonical commit stream is the source of truth; row state is a rebuildable projection. Kubernetes owns lifecycle through an operator-lite and manifests, while OpenDB owns data correctness and root-range replication semantics.

**Tech Stack:** Rust, Tokio, Serde, kube-rs, k8s-openapi, Schemars, TypeScript, Vitest, tsx, k3s, Kubernetes StatefulSet/PVC/Service/PDB manifests. No Python.

---

## Scope Check

This plan implements only Milestone 1 from `docs/superpowers/specs/2026-05-05-opendb-design.md`.

It does not implement multi-range routing, range split/merge, cross-range transactions, columnar projections, GIS, Supabase-like platform APIs, object archival, or deep PostgreSQL dialect compatibility. It creates interfaces that do not block those later phases.

## Important Decision For Execution

Consensus has two credible execution paths:

- **Recommended for Milestone 1:** use OpenRaft behind an `opendb-consensus` facade. This gives correct leader election, log replication, and membership semantics while OpenDB still owns the data model, WAL, commit stream, SQL, pgwire, operator, and projections.
- **Alternative:** implement a root-range majority log in-house first. This gives more control but is easier to get subtly wrong and should only be chosen with a stronger fault-injection test budget.

This plan chooses the recommended path: OpenRaft behind an internal facade. OpenRaft is not a database component and does not become the source of truth; it is the consensus engine for the root range.

Execution gate: do not claim Milestone 1 complete until root-range writes flow through `opendb-consensus`. Early tasks may use direct WAL or in-memory SQL only to establish the API surface, but final verification must prove the consensus boundary is on the write path.

Reference docs checked during planning:

- kube-rs `CustomResource`: https://docs.rs/kube/latest/kube/derive.CustomResource.html
- OpenRaft: https://docs.rs/openraft/latest/openraft/

## Repository Notes

The workspace currently has a read-only invalid `.git` directory. Until that is replaced with a normal repository, use:

```bash
git --git-dir=.opendb-git --work-tree=. status --short
```

For commits in this environment, use:

```bash
git --git-dir=.opendb-git --work-tree=. add <files>
git --git-dir=.opendb-git --work-tree=. commit -m "<message>"
```

When executing from a real Git worktree such as `.worktrees/feat-milestone-1`, use normal Git commands instead:

```bash
git add <files>
git commit -m "<message>"
```

## File Structure

Create this structure:

```text
Cargo.toml
rust-toolchain.toml
package.json
tsconfig.json
vitest.config.ts
crates/opendb-common/src/lib.rs
crates/opendb-common/src/ids.rs
crates/opendb-common/src/error.rs
crates/opendb-storage/src/lib.rs
crates/opendb-storage/src/commit_stream.rs
crates/opendb-storage/src/wal.rs
crates/opendb-storage/src/row_projection.rs
crates/opendb-sql/src/lib.rs
crates/opendb-sql/src/ast.rs
crates/opendb-sql/src/parser.rs
crates/opendb-sql/src/executor.rs
crates/opendb-consensus/src/lib.rs
crates/opendb-consensus/src/root_range.rs
crates/opendb-node/src/main.rs
crates/opendb-node/src/config.rs
crates/opendb-node/src/health.rs
crates/opendb-node/src/pgwire.rs
crates/opendb-operator/src/main.rs
crates/opendb-operator/src/crd.rs
deploy/k8s/base/opendb-cluster.yaml
deploy/k8s/base/serviceaccount.yaml
deploy/k8s/base/rbac.yaml
deploy/k8s/base/services.yaml
deploy/k8s/base/statefulset.yaml
tools/no-python.ts
tools/check-manifests.ts
tools/pgwire-smoke.ts
tests/cluster/manifests.test.ts
tests/parity/sql-smoke.test.ts
```

Responsibilities:

- `opendb-common`: stable IDs, shared errors, result aliases.
- `opendb-storage`: commit stream v1, WAL encode/decode, replay into row projection.
- `opendb-sql`: minimal SQL AST, parser, and executor over the root projection API.
- `opendb-consensus`: root-range consensus facade; OpenRaft integration lives behind this boundary.
- `opendb-node`: executable DB node exposing health and pgwire-compatible endpoints.
- `opendb-operator`: CRD definition and minimal reconciliation entrypoint.
- `deploy/k8s/base`: k3s-compatible manifests for a 3-node cluster shape.
- `tools` and `tests`: TypeScript verification only.

---

### Task 1: Workspace And No-Python Guard

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `vitest.config.ts`
- Create: `tools/no-python.ts`
- Modify: `.gitignore`

- [ ] **Step 1: Create the Rust workspace manifest**

Create `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
  "crates/opendb-common",
  "crates/opendb-storage",
  "crates/opendb-sql",
  "crates/opendb-consensus",
  "crates/opendb-node",
  "crates/opendb-operator",
]

[workspace.package]
edition = "2024"
license = "Apache-2.0"
repository = "https://example.invalid/opendb"

[workspace.dependencies]
anyhow = "1"
bytes = "1"
clap = { version = "4", features = ["derive", "env"] }
futures = "0.3"
k8s-openapi = { version = "0.25", features = ["latest"] }
kube = { version = "1", features = ["client", "derive", "runtime"] }
openraft = { version = "0.9", features = ["serde"] }
schemars = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
thiserror = "2"
tokio = { version = "1", features = ["fs", "io-util", "macros", "net", "rt-multi-thread", "signal", "sync", "time"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 2: Pin the Rust channel**

Create `rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
components = ["clippy", "rustfmt"]
```

- [ ] **Step 3: Create TypeScript tooling config**

Create `package.json`:

```json
{
  "name": "opendb",
  "private": true,
  "type": "module",
  "scripts": {
    "check:no-python": "tsx tools/no-python.ts",
    "check:manifests": "tsx tools/check-manifests.ts",
    "test": "vitest run",
    "test:parity": "vitest run tests/parity",
    "test:cluster": "vitest run tests/cluster"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "tsx": "^4.0.0",
    "typescript": "^5.0.0",
    "vitest": "^3.0.0",
    "yaml": "^2.0.0"
  }
}
```

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "resolveJsonModule": true,
    "types": ["node", "vitest/globals"]
  },
  "include": ["tools/**/*.ts", "tests/**/*.ts", "vitest.config.ts"]
}
```

Create `vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    include: ["tests/**/*.test.ts"],
    pool: "forks",
    testTimeout: 30_000
  }
});
```

- [ ] **Step 4: Add the no-Python guard**

Create `tools/no-python.ts`:

```ts
import { readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = process.cwd();
const ignored = new Set([".git", ".opendb-git", "target", "node_modules", ".superpowers", ".playwright-mcp"]);
const forbiddenExtensions = new Set([".py", ".pyi", ".pyw"]);
const forbiddenFiles: string[] = [];

function walk(dir: string): void {
  for (const entry of readdirSync(dir)) {
    if (ignored.has(entry)) continue;
    const path = join(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      walk(path);
      continue;
    }
    for (const ext of forbiddenExtensions) {
      if (entry.endsWith(ext)) {
        forbiddenFiles.push(relative(root, path));
      }
    }
  }
}

walk(root);

if (forbiddenFiles.length > 0) {
  console.error("Python files are not allowed in OpenDB:");
  for (const file of forbiddenFiles) console.error(`- ${file}`);
  process.exit(1);
}

console.log("No Python files found.");
```

- [ ] **Step 5: Ensure generated and local files are ignored**

Make `.gitignore` contain at least:

```gitignore
.opendb-git/
.superpowers/
.playwright-mcp/
target/
node_modules/
dist/
*.swp
.env
.env.*
```

- [ ] **Step 6: Run workspace checks**

Run:

```bash
npm install
npm run check:no-python
cargo metadata --no-deps
```

Expected:

```text
No Python files found.
```

`cargo metadata --no-deps` may fail until crate manifests are created in Task 2. If it fails only because member manifests are missing, continue to Task 2.

- [ ] **Step 7: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add .gitignore Cargo.toml rust-toolchain.toml package.json tsconfig.json vitest.config.ts tools/no-python.ts
git --git-dir=.opendb-git --work-tree=. commit -m "chore: scaffold workspace tooling"
```

---

### Task 2: Crate Skeletons And Common Types

**Files:**
- Create: `crates/opendb-common/Cargo.toml`
- Create: `crates/opendb-common/src/lib.rs`
- Create: `crates/opendb-common/src/ids.rs`
- Create: `crates/opendb-common/src/error.rs`
- Create: crate manifests and `src/lib.rs` files for `opendb-storage`, `opendb-sql`, `opendb-consensus`
- Create: `crates/opendb-node/Cargo.toml`
- Create: `crates/opendb-operator/Cargo.toml`

- [ ] **Step 1: Create the common crate**

Create `crates/opendb-common/Cargo.toml`:

```toml
[package]
name = "opendb-common"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
```

Create `crates/opendb-common/src/lib.rs`:

```rust
pub mod error;
pub mod ids;

pub use error::{OpenDbError, OpenDbResult};
pub use ids::{LogicalTimestamp, NodeId, RangeId, TransactionId};
```

Create `crates/opendb-common/src/error.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum OpenDbError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("sql error: {0}")]
    Sql(String),
}

pub type OpenDbResult<T> = Result<T, OpenDbError>;
```

Create `crates/opendb-common/src/ids.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RangeId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TransactionId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogicalTimestamp(pub u64);

impl RangeId {
    pub const ROOT: Self = Self(1);
}
```

- [ ] **Step 2: Create the remaining crate manifests**

Create `crates/opendb-storage/Cargo.toml`:

```toml
[package]
name = "opendb-storage"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
opendb-common = { path = "../opendb-common" }
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true

[dev-dependencies]
tempfile = "3"
```

Create `crates/opendb-sql/Cargo.toml`:

```toml
[package]
name = "opendb-sql"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
opendb-common = { path = "../opendb-common" }
opendb-storage = { path = "../opendb-storage" }
serde.workspace = true
```

Create `crates/opendb-consensus/Cargo.toml`:

```toml
[package]
name = "opendb-consensus"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
opendb-common = { path = "../opendb-common" }
opendb-storage = { path = "../opendb-storage" }
openraft.workspace = true
serde.workspace = true
tokio.workspace = true
```

Create `crates/opendb-node/Cargo.toml`:

```toml
[package]
name = "opendb-node"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[[bin]]
name = "opendb-node"
path = "src/main.rs"

[dependencies]
anyhow.workspace = true
bytes.workspace = true
clap.workspace = true
opendb-common = { path = "../opendb-common" }
opendb-consensus = { path = "../opendb-consensus" }
opendb-sql = { path = "../opendb-sql" }
opendb-storage = { path = "../opendb-storage" }
tokio.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
```

Create `crates/opendb-operator/Cargo.toml`:

```toml
[package]
name = "opendb-operator"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[[bin]]
name = "opendb-operator"
path = "src/main.rs"

[dependencies]
anyhow.workspace = true
k8s-openapi.workspace = true
kube.workspace = true
schemars.workspace = true
serde.workspace = true
serde_json.workspace = true
serde_yaml.workspace = true
tokio.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
```

- [ ] **Step 3: Add minimal lib roots**

Create `crates/opendb-storage/src/lib.rs`:

```rust
pub mod commit_stream;
pub mod row_projection;
pub mod wal;
```

Create `crates/opendb-sql/src/lib.rs`:

```rust
pub mod ast;
pub mod executor;
pub mod parser;
```

Create `crates/opendb-consensus/src/lib.rs`:

```rust
pub mod root_range;
```

- [ ] **Step 4: Verify the workspace compiles to the expected missing-module errors**

Run:

```bash
cargo check
```

Expected: failure only for missing files referenced by the new `lib.rs` files, such as `file not found for module commit_stream`.

- [ ] **Step 5: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates Cargo.toml
git --git-dir=.opendb-git --work-tree=. commit -m "chore: add rust crate skeletons"
```

---

### Task 3: Commit Stream V1 And WAL

**Files:**
- Create: `crates/opendb-storage/src/commit_stream.rs`
- Create: `crates/opendb-storage/src/wal.rs`
- Modify: `crates/opendb-storage/src/lib.rs`

- [ ] **Step 1: Write commit stream tests**

Add to `crates/opendb-storage/src/commit_stream.rs`:

```rust
use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnValue {
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    CreateTable { table: String, columns: Vec<String> },
    InsertRow { table: String, key: String, values: Vec<ColumnValue> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub version: u16,
    pub tx_id: TransactionId,
    pub range_id: RangeId,
    pub ts: LogicalTimestamp,
    pub actor: String,
    pub mutations: Vec<Mutation>,
}

impl CommitRecord {
    pub const VERSION: u16 = 1;

    pub fn new(tx_id: TransactionId, ts: LogicalTimestamp, mutations: Vec<Mutation>) -> Self {
        Self {
            version: Self::VERSION,
            tx_id,
            range_id: RangeId::ROOT,
            ts,
            actor: "system".to_owned(),
            mutations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_record_has_stable_version_and_root_range() {
        let record = CommitRecord::new(
            TransactionId(7),
            LogicalTimestamp(11),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "name".to_owned()],
            }],
        );

        assert_eq!(record.version, 1);
        assert_eq!(record.range_id, RangeId::ROOT);
        assert_eq!(record.actor, "system");
    }
}
```

- [ ] **Step 2: Run the commit stream test**

Run:

```bash
cargo test -p opendb-storage commit_record_has_stable_version_and_root_range
```

Expected: PASS.

- [ ] **Step 3: Add WAL tests and implementation**

Create `crates/opendb-storage/src/wal.rs`:

```rust
use crate::commit_stream::CommitRecord;
use opendb_common::{OpenDbError, OpenDbResult};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Clone, Debug)]
pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub async fn append(&self, record: &CommitRecord) -> OpenDbResult<()> {
        ensure_parent(&self.path).await?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
        let line = serde_json::to_string(record).map_err(|err| OpenDbError::Storage(err.to_string()))?;
        file.write_all(line.as_bytes())
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
        file.write_all(b"\n")
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
        file.sync_data()
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
        Ok(())
    }

    pub async fn read_all(&self) -> OpenDbResult<Vec<CommitRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut records = Vec::new();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }
            let record = serde_json::from_str(&line).map_err(|err| OpenDbError::Storage(err.to_string()))?;
            records.push(record);
        }
        Ok(records)
    }
}

async fn ensure_parent(path: &Path) -> OpenDbResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| OpenDbError::Storage(err.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{Mutation, Value, ColumnValue};
    use opendb_common::{LogicalTimestamp, TransactionId};

    #[tokio::test]
    async fn wal_appends_and_reads_records_in_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal = Wal::new(dir.path().join("root-range.wal"));
        let first = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "name".to_owned()],
            }],
        );
        let second = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![ColumnValue {
                    column: "name".to_owned(),
                    value: Value::Text("Ada".to_owned()),
                }],
            }],
        );

        wal.append(&first).await.expect("append first");
        wal.append(&second).await.expect("append second");

        assert_eq!(wal.read_all().await.expect("read all"), vec![first, second]);
    }
}
```

- [ ] **Step 4: Run WAL tests**

Run:

```bash
cargo test -p opendb-storage wal_appends_and_reads_records_in_order
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-storage/src/commit_stream.rs crates/opendb-storage/src/wal.rs
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add commit stream and wal"
```

---

### Task 4: Rebuildable Row Projection

**Files:**
- Create: `crates/opendb-storage/src/row_projection.rs`
- Modify: `crates/opendb-storage/src/lib.rs`

- [ ] **Step 1: Add row projection implementation and replay test**

Create `crates/opendb-storage/src/row_projection.rs`:

```rust
use crate::commit_stream::{CommitRecord, Mutation, Value};
use opendb_common::{OpenDbError, OpenDbResult};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: BTreeMap<String, BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowProjection {
    tables: BTreeMap<String, Table>,
}

impl RowProjection {
    pub fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        for mutation in &record.mutations {
            match mutation {
                Mutation::CreateTable { table, columns } => {
                    if self.tables.contains_key(table) {
                        return Err(OpenDbError::InvalidInput(format!("table already exists: {table}")));
                    }
                    self.tables.insert(
                        table.clone(),
                        Table {
                            columns: columns.clone(),
                            rows: BTreeMap::new(),
                        },
                    );
                }
                Mutation::InsertRow { table, key, values } => {
                    let table_state = self
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                    let mut row = BTreeMap::new();
                    for column_value in values {
                        if !table_state.columns.contains(&column_value.column) {
                            return Err(OpenDbError::InvalidInput(format!(
                                "unknown column {} on table {}",
                                column_value.column, table
                            )));
                        }
                        row.insert(column_value.column.clone(), column_value.value.clone());
                    }
                    table_state.rows.insert(key.clone(), row);
                }
            }
        }
        Ok(())
    }

    pub fn rebuild(records: &[CommitRecord]) -> OpenDbResult<Self> {
        let mut projection = Self::default();
        for record in records {
            projection.apply(record)?;
        }
        Ok(projection)
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{ColumnValue, Mutation, Value};
    use opendb_common::{LogicalTimestamp, TransactionId};

    #[test]
    fn row_projection_rebuilds_from_commit_stream() {
        let records = vec![
            CommitRecord::new(
                TransactionId(1),
                LogicalTimestamp(1),
                vec![Mutation::CreateTable {
                    table: "accounts".to_owned(),
                    columns: vec!["id".to_owned(), "name".to_owned()],
                }],
            ),
            CommitRecord::new(
                TransactionId(2),
                LogicalTimestamp(2),
                vec![Mutation::InsertRow {
                    table: "accounts".to_owned(),
                    key: "1".to_owned(),
                    values: vec![
                        ColumnValue { column: "id".to_owned(), value: Value::Int64(1) },
                        ColumnValue { column: "name".to_owned(), value: Value::Text("Ada".to_owned()) },
                    ],
                }],
            ),
        ];

        let projection = RowProjection::rebuild(&records).expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts table");
        assert_eq!(accounts.rows.len(), 1);
        assert_eq!(
            accounts.rows.get("1").and_then(|row| row.get("name")),
            Some(&Value::Text("Ada".to_owned()))
        );
    }
}
```

- [ ] **Step 2: Run projection tests**

Run:

```bash
cargo test -p opendb-storage row_projection_rebuilds_from_commit_stream
```

Expected: PASS.

- [ ] **Step 3: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-storage/src/row_projection.rs
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add rebuildable row projection"
```

---

### Task 5: Minimal SQL AST, Parser, And Executor

**Files:**
- Create: `crates/opendb-sql/src/ast.rs`
- Create: `crates/opendb-sql/src/parser.rs`
- Create: `crates/opendb-sql/src/executor.rs`

- [ ] **Step 1: Add AST types**

Create `crates/opendb-sql/src/ast.rs`:

```rust
use opendb_storage::commit_stream::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    CreateTable { table: String, columns: Vec<String> },
    Insert { table: String, values: Vec<Value> },
    SelectAll { table: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResult {
    Command { tag: String },
    Rows { columns: Vec<String>, rows: Vec<Vec<Value>> },
}
```

- [ ] **Step 2: Add parser tests and implementation**

Create `crates/opendb-sql/src/parser.rs`:

```rust
use crate::ast::Statement;
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::Value;

pub fn parse(sql: &str) -> OpenDbResult<Statement> {
    let normalized = sql.trim().trim_end_matches(';').trim();
    let upper = normalized.to_ascii_uppercase();
    if upper.starts_with("CREATE TABLE ") {
        parse_create_table(normalized)
    } else if upper.starts_with("INSERT INTO ") {
        parse_insert(normalized)
    } else if upper.starts_with("SELECT * FROM ") {
        parse_select_all(normalized)
    } else {
        Err(OpenDbError::Sql(format!("unsupported SQL: {normalized}")))
    }
}

fn parse_create_table(sql: &str) -> OpenDbResult<Statement> {
    let rest = sql
        .strip_prefix("CREATE TABLE ")
        .or_else(|| sql.strip_prefix("create table "))
        .ok_or_else(|| OpenDbError::Sql("invalid CREATE TABLE".to_owned()))?;
    let open = rest.find('(').ok_or_else(|| OpenDbError::Sql("missing column list".to_owned()))?;
    let close = rest.rfind(')').ok_or_else(|| OpenDbError::Sql("missing closing paren".to_owned()))?;
    let table = rest[..open].trim().to_owned();
    let columns = rest[open + 1..close]
        .split(',')
        .map(|part| part.trim().split_whitespace().next().unwrap_or("").to_owned())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    if table.is_empty() || columns.is_empty() {
        return Err(OpenDbError::Sql("CREATE TABLE requires table and columns".to_owned()));
    }
    Ok(Statement::CreateTable { table, columns })
}

fn parse_insert(sql: &str) -> OpenDbResult<Statement> {
    let values_marker = " VALUES ";
    let values_pos = sql
        .to_ascii_uppercase()
        .find(values_marker)
        .ok_or_else(|| OpenDbError::Sql("INSERT requires VALUES".to_owned()))?;
    let table = sql["INSERT INTO ".len()..values_pos].trim().to_owned();
    let values_part = sql[values_pos + values_marker.len()..].trim();
    let open = values_part.find('(').ok_or_else(|| OpenDbError::Sql("missing values open paren".to_owned()))?;
    let close = values_part.rfind(')').ok_or_else(|| OpenDbError::Sql("missing values close paren".to_owned()))?;
    let values = values_part[open + 1..close]
        .split(',')
        .map(parse_value)
        .collect::<OpenDbResult<Vec<_>>>()?;
    Ok(Statement::Insert { table, values })
}

fn parse_value(raw: &str) -> OpenDbResult<Value> {
    let value = raw.trim();
    if let Some(text) = value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')) {
        return Ok(Value::Text(text.to_owned()));
    }
    value
        .parse::<i64>()
        .map(Value::Int64)
        .map_err(|_| OpenDbError::Sql(format!("unsupported literal: {value}")))
}

fn parse_select_all(sql: &str) -> OpenDbResult<Statement> {
    let table = sql["SELECT * FROM ".len()..].trim().to_owned();
    if table.is_empty() {
        return Err(OpenDbError::Sql("SELECT requires table".to_owned()));
    }
    Ok(Statement::SelectAll { table })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_insert_and_select_subset() {
        assert_eq!(
            parse("CREATE TABLE accounts (id INT, name TEXT);").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "name".to_owned()],
            }
        );
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
        assert_eq!(
            parse("SELECT * FROM accounts").expect("select"),
            Statement::SelectAll { table: "accounts".to_owned() }
        );
    }
}
```

- [ ] **Step 3: Add executor tests and implementation**

Create `crates/opendb-sql/src/executor.rs`:

```rust
use crate::ast::{QueryResult, Statement};
use opendb_common::{LogicalTimestamp, OpenDbError, OpenDbResult, TransactionId};
use opendb_storage::commit_stream::{ColumnValue, CommitRecord, Mutation, Value};
use opendb_storage::row_projection::RowProjection;

#[derive(Debug, Default)]
pub struct SqlEngine {
    next_tx: u64,
    projection: RowProjection,
    commits: Vec<CommitRecord>,
}

impl SqlEngine {
    pub fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        match statement {
            Statement::CreateTable { table, columns } => {
                let record = self.next_record(vec![Mutation::CreateTable { table, columns }]);
                self.apply(record)?;
                Ok(QueryResult::Command { tag: "CREATE TABLE".to_owned() })
            }
            Statement::Insert { table, values } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                if values.len() != table_state.columns.len() {
                    return Err(OpenDbError::Sql(format!(
                        "expected {} values, got {}",
                        table_state.columns.len(),
                        values.len()
                    )));
                }
                let row_key = match values.first() {
                    Some(Value::Int64(value)) => value.to_string(),
                    Some(Value::Text(value)) => value.clone(),
                    None => return Err(OpenDbError::Sql("INSERT requires at least one value".to_owned())),
                };
                let column_values = table_state
                    .columns
                    .iter()
                    .cloned()
                    .zip(values)
                    .map(|(column, value)| ColumnValue { column, value })
                    .collect();
                let record = self.next_record(vec![Mutation::InsertRow {
                    table,
                    key: row_key,
                    values: column_values,
                }]);
                self.apply(record)?;
                Ok(QueryResult::Command { tag: "INSERT 0 1".to_owned() })
            }
            Statement::SelectAll { table } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                let rows = table_state
                    .rows
                    .values()
                    .map(|row| {
                        table_state
                            .columns
                            .iter()
                            .map(|column| row.get(column).cloned().unwrap_or(Value::Text(String::new())))
                            .collect()
                    })
                    .collect();
                Ok(QueryResult::Rows {
                    columns: table_state.columns.clone(),
                    rows,
                })
            }
        }
    }

    pub fn commits(&self) -> &[CommitRecord] {
        &self.commits
    }

    fn next_record(&mut self, mutations: Vec<Mutation>) -> CommitRecord {
        self.next_tx += 1;
        CommitRecord::new(
            TransactionId(self.next_tx),
            LogicalTimestamp(self.next_tx),
            mutations,
        )
    }

    fn apply(&mut self, record: CommitRecord) -> OpenDbResult<()> {
        self.projection.apply(&record)?;
        self.commits.push(record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn executes_create_insert_select_against_row_projection() {
        let mut engine = SqlEngine::default();
        assert_eq!(
            engine.execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse")).expect("create"),
            QueryResult::Command { tag: "CREATE TABLE".to_owned() }
        );
        assert_eq!(
            engine.execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse")).expect("insert"),
            QueryResult::Command { tag: "INSERT 0 1".to_owned() }
        );
        assert_eq!(
            engine.execute(parse("SELECT * FROM accounts").expect("parse")).expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );
        assert_eq!(engine.commits().len(), 2);
    }
}
```

- [ ] **Step 4: Run SQL tests**

Run:

```bash
cargo test -p opendb-sql
```

Expected: all `opendb-sql` tests PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-sql/src
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add minimal sql execution"
```

---

### Task 6: Node Health Server And Minimal Pgwire Query Path

**Files:**
- Create: `crates/opendb-node/src/config.rs`
- Create: `crates/opendb-node/src/health.rs`
- Create: `crates/opendb-node/src/pgwire.rs`
- Create: `crates/opendb-node/src/main.rs`
- Create: `tools/pgwire-smoke.ts`

- [ ] **Step 1: Add node config**

Create `crates/opendb-node/src/config.rs`:

```rust
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
pub struct NodeConfig {
    #[arg(long, env = "OPENDB_NODE_ID")]
    pub node_id: u64,
    #[arg(long, env = "OPENDB_DATA_DIR", default_value = "/var/lib/opendb")]
    pub data_dir: PathBuf,
    #[arg(long, env = "OPENDB_PGWIRE_ADDR", default_value = "0.0.0.0:5432")]
    pub pgwire_addr: String,
    #[arg(long, env = "OPENDB_HEALTH_ADDR", default_value = "0.0.0.0:8080")]
    pub health_addr: String,
}
```

- [ ] **Step 2: Add health endpoint**

Create `crates/opendb-node/src/health.rs`:

```rust
use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn serve_health(addr: String) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            let _ = socket.read(&mut buffer).await;
            let body = "ok\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: text/plain\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}
```

- [ ] **Step 3: Add minimal pgwire endpoint**

Create `crates/opendb-node/src/pgwire.rs`:

```rust
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use opendb_sql::ast::{QueryResult, Statement};
use opendb_sql::executor::SqlEngine;
use opendb_sql::parser::parse;
use opendb_storage::commit_stream::Value;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub async fn serve_pgwire(addr: String) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    let engine = Arc::new(Mutex::new(SqlEngine::default()));
    loop {
        let (socket, _) = listener.accept().await?;
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            let _ = handle_client(socket, engine).await;
        });
    }
}

async fn handle_client(mut socket: TcpStream, engine: Arc<Mutex<SqlEngine>>) -> Result<()> {
    let mut startup = [0_u8; 1024];
    let read = socket.read(&mut startup).await?;
    if read >= 8 {
        let code = i32::from_be_bytes([startup[4], startup[5], startup[6], startup[7]]);
        if code == 80877103 {
            socket.write_all(b"N").await?;
            let _ = socket.read(&mut startup).await?;
        }
    }
    socket.write_all(&authentication_ok()).await?;
    socket.write_all(&parameter_status("server_version", "OpenDB 0.1")).await?;
    socket.write_all(&parameter_status("client_encoding", "UTF8")).await?;
    socket.write_all(&ready()).await?;

    loop {
        let mut header = [0_u8; 5];
        if socket.read_exact(&mut header).await.is_err() {
            return Ok(());
        }
        let tag = header[0];
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut payload = vec![0_u8; len.saturating_sub(4)];
        socket.read_exact(&mut payload).await?;
        if tag == b'Q' {
            let sql = String::from_utf8_lossy(&payload);
            let sql = sql.trim_end_matches('\0').to_owned();
            let response = execute_query(&sql, &engine).await;
            socket.write_all(&response).await?;
            socket.write_all(&ready()).await?;
        }
    }
}

async fn execute_query(sql: &str, engine: &Arc<Mutex<SqlEngine>>) -> Vec<u8> {
    let statement = match parse(sql) {
        Ok(statement) => statement,
        Err(err) => return error_response(&err.to_string()),
    };
    let mut engine = engine.lock().await;
    match engine.execute(statement) {
        Ok(QueryResult::Command { tag }) => command_complete(&tag),
        Ok(QueryResult::Rows { columns, rows }) => rows_response(columns, rows),
        Err(err) => error_response(&err.to_string()),
    }
}

fn authentication_ok() -> Vec<u8> {
    let mut out = BytesMut::new();
    out.put_u8(b'R');
    out.put_i32(8);
    out.put_i32(0);
    out.to_vec()
}

fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(value.as_bytes());
    payload.put_u8(0);
    let mut out = BytesMut::new();
    out.put_u8(b'S');
    out.put_i32((payload.len() + 4) as i32);
    out.extend_from_slice(&payload);
    out.to_vec()
}

fn ready() -> Vec<u8> {
    vec![b'Z', 0, 0, 0, 5, b'I']
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(tag.as_bytes());
    payload.put_u8(0);
    let mut out = BytesMut::new();
    out.put_u8(b'C');
    out.put_i32((payload.len() + 4) as i32);
    out.extend_from_slice(&payload);
    out.to_vec()
}

fn rows_response(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.extend_from_slice(&row_description(&columns));
    for row in rows {
        out.extend_from_slice(&data_row(row));
    }
    out.extend_from_slice(&command_complete("SELECT 1"));
    out.to_vec()
}

fn row_description(columns: &[String]) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_i16(columns.len() as i16);
    for column in columns {
        payload.extend_from_slice(column.as_bytes());
        payload.put_u8(0);
        payload.put_i32(0);
        payload.put_i16(0);
        payload.put_i32(25);
        payload.put_i16(-1);
        payload.put_i32(-1);
        payload.put_i16(0);
    }
    let mut out = BytesMut::new();
    out.put_u8(b'T');
    out.put_i32((payload.len() + 4) as i32);
    out.extend_from_slice(&payload);
    out.to_vec()
}

fn data_row(values: Vec<Value>) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_i16(values.len() as i16);
    for value in values {
        let text = match value {
            Value::Int64(value) => value.to_string(),
            Value::Text(value) => value,
        };
        payload.put_i32(text.len() as i32);
        payload.extend_from_slice(text.as_bytes());
    }
    let mut out = BytesMut::new();
    out.put_u8(b'D');
    out.put_i32((payload.len() + 4) as i32);
    out.extend_from_slice(&payload);
    out.to_vec()
}

fn error_response(message: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(b"ERROR");
    payload.put_u8(0);
    payload.put_u8(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.put_u8(0);
    payload.put_u8(0);
    let mut out = BytesMut::new();
    out.put_u8(b'E');
    out.put_i32((payload.len() + 4) as i32);
    out.extend_from_slice(&payload);
    out.to_vec()
}
```

- [ ] **Step 4: Add node main**

Create `crates/opendb-node/src/main.rs`:

```rust
mod config;
mod health;
mod pgwire;

use anyhow::Result;
use clap::Parser;
use config::NodeConfig;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = NodeConfig::parse();
    tokio::try_join!(
        health::serve_health(config.health_addr),
        pgwire::serve_pgwire(config.pgwire_addr),
    )?;
    Ok(())
}
```

- [ ] **Step 5: Add a TypeScript pgwire smoke script**

Create `tools/pgwire-smoke.ts`:

```ts
import net from "node:net";

const host = process.env.OPENDB_PGWIRE_HOST || "127.0.0.1";
const port = Number(process.env.OPENDB_PGWIRE_PORT || "5432");

function int32(value: number): Buffer {
  const buf = Buffer.alloc(4);
  buf.writeInt32BE(value, 0);
  return buf;
}

function startup(): Buffer {
  const params = Buffer.from("user\0opendb\0database\0opendb\0\0");
  return Buffer.concat([int32(params.length + 8), int32(196608), params]);
}

function query(sql: string): Buffer {
  const payload = Buffer.from(`${sql}\0`);
  return Buffer.concat([Buffer.from("Q"), int32(payload.length + 4), payload]);
}

const socket = net.connect({ host, port });
const chunks: Buffer[] = [];

socket.on("connect", () => {
  socket.write(startup());
  socket.write(query("CREATE TABLE accounts (id INT, name TEXT)"));
  socket.write(query("INSERT INTO accounts VALUES (1, 'Ada')"));
  socket.write(query("SELECT * FROM accounts"));
});

socket.on("data", (chunk) => {
  chunks.push(chunk);
  const text = Buffer.concat(chunks).toString("utf8");
  if (text.includes("Ada")) {
    console.log("pgwire smoke passed");
    socket.end();
  }
});

socket.on("close", () => {
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text.includes("Ada")) {
    console.error(text);
    process.exit(1);
  }
});

socket.on("error", (error) => {
  console.error(error);
  process.exit(1);
});
```

- [ ] **Step 6: Run node checks**

Run:

```bash
cargo check -p opendb-node
cargo run -p opendb-node -- --node-id 1 --pgwire-addr 127.0.0.1:55432 --health-addr 127.0.0.1:58080
```

In another terminal, run:

```bash
OPENDB_PGWIRE_PORT=55432 npm exec tsx tools/pgwire-smoke.ts
```

Expected:

```text
pgwire smoke passed
```

- [ ] **Step 7: Stop the local node process and commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-node/src tools/pgwire-smoke.ts
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add node health and pgwire smoke path"
```

---

### Task 7: Root-Range Consensus Facade

**Files:**
- Create: `crates/opendb-consensus/src/root_range.rs`

- [ ] **Step 1: Add root-range facade tests and implementation**

Create `crates/opendb-consensus/src/root_range.rs`:

```rust
use opendb_common::{OpenDbResult, RangeId};
use opendb_storage::commit_stream::CommitRecord;
use opendb_storage::wal::Wal;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct RootRange {
    range_id: RangeId,
    wal: Wal,
}

impl RootRange {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.join("root-range").join("commit.wal")),
        }
    }

    pub fn range_id(&self) -> RangeId {
        self.range_id
    }

    pub async fn append_committed(&self, record: &CommitRecord) -> OpenDbResult<()> {
        self.wal.append(record).await
    }

    pub async fn replay(&self) -> OpenDbResult<Vec<CommitRecord>> {
        self.wal.read_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendb_common::{LogicalTimestamp, TransactionId};
    use opendb_storage::commit_stream::Mutation;

    #[tokio::test]
    async fn root_range_replays_committed_records_after_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let first = RootRange::new(dir.path());
        let record = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned()],
            }],
        );

        first.append_committed(&record).await.expect("append");
        let restarted = RootRange::new(dir.path());

        assert_eq!(restarted.range_id(), RangeId::ROOT);
        assert_eq!(restarted.replay().await.expect("replay"), vec![record]);
    }
}
```

- [ ] **Step 2: Add the missing dev dependency**

Add to `crates/opendb-consensus/Cargo.toml`:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Run root-range tests**

Run:

```bash
cargo test -p opendb-consensus root_range_replays_committed_records_after_restart
```

Expected: PASS.

- [ ] **Step 4: Integrate OpenRaft behind this facade**

Add an internal design note to `crates/opendb-consensus/src/root_range.rs` above `RootRange`:

```rust
// Milestone 1 keeps the public consensus boundary here. OpenRaft integration
// must stay behind RootRange so SQL, storage, pgwire, and Kubernetes code do
// not depend directly on OpenRaft types.
```

Then add the first OpenRaft adapter types under the comment:

```rust
pub type OpenDbRaftNodeId = u64;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RootRangeCommand {
    pub record: CommitRecord,
}
```

Do not expose OpenRaft types outside `opendb-consensus`.

- [ ] **Step 5: Run consensus checks**

Run:

```bash
cargo check -p opendb-consensus
cargo test -p opendb-consensus
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-consensus
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add root range consensus facade"
```

---

### Task 8: Operator-Lite CRD

**Files:**
- Create: `crates/opendb-operator/src/crd.rs`
- Create: `crates/opendb-operator/src/main.rs`

- [ ] **Step 1: Add CRD type**

Create `crates/opendb-operator/src/crd.rs`:

```rust
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "db.opendb.dev",
    version = "v1alpha1",
    kind = "OpenDbCluster",
    plural = "opendbclusters",
    namespaced,
    status = "OpenDbClusterStatus",
    derive = "PartialEq",
    shortname = "odb"
)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterSpec {
    pub replicas: i32,
    pub image: String,
    pub storage_class_name: String,
    pub storage_size: String,
    pub pgwire_port: i32,
    pub health_port: i32,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterStatus {
    pub ready_replicas: i32,
    pub phase: String,
}
```

- [ ] **Step 2: Add CRD generation command**

Create `crates/opendb-operator/src/main.rs`:

```rust
mod crd;

use anyhow::Result;
use clap::Parser;
use kube::CustomResourceExt;

#[derive(Debug, Parser)]
enum Command {
    PrintCrd,
    Run,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Command::parse() {
        Command::PrintCrd => {
            println!("{}", serde_yaml::to_string(&crd::OpenDbCluster::crd())?);
        }
        Command::Run => {
            tracing::info!("opendb operator-lite started");
            tokio::signal::ctrl_c().await?;
        }
    }

    Ok(())
}
```

- [ ] **Step 3: Ensure `clap` is available to operator**

Add to `crates/opendb-operator/Cargo.toml` dependencies:

```toml
clap.workspace = true
```

- [ ] **Step 4: Run CRD generation**

Run:

```bash
cargo run -p opendb-operator -- print-crd > /tmp/opendbcluster-crd.yaml
```

Expected: `/tmp/opendbcluster-crd.yaml` contains `kind: CustomResourceDefinition` and `kind: OpenDbCluster`.

- [ ] **Step 5: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add crates/opendb-operator
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add opendb cluster crd"
```

---

### Task 9: k3s-Compatible Manifests And Manifest Tests

**Files:**
- Create: `deploy/k8s/base/opendb-cluster.yaml`
- Create: `deploy/k8s/base/serviceaccount.yaml`
- Create: `deploy/k8s/base/rbac.yaml`
- Create: `deploy/k8s/base/services.yaml`
- Create: `deploy/k8s/base/statefulset.yaml`
- Create: `tools/check-manifests.ts`
- Create: `tests/cluster/manifests.test.ts`

- [ ] **Step 1: Add cluster custom resource**

Create `deploy/k8s/base/opendb-cluster.yaml`:

```yaml
apiVersion: db.opendb.dev/v1alpha1
kind: OpenDbCluster
metadata:
  name: opendb
  namespace: opendb-system
spec:
  replicas: 3
  image: opendb-node:dev
  storageClassName: local-path
  storageSize: 1Gi
  pgwirePort: 5432
  healthPort: 8080
```

- [ ] **Step 2: Add Kubernetes service account and RBAC**

Create `deploy/k8s/base/serviceaccount.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opendb-operator
  namespace: opendb-system
```

Create `deploy/k8s/base/rbac.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: opendb-operator
  namespace: opendb-system
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["db.opendb.dev"]
    resources: ["opendbclusters", "opendbclusters/status"]
    verbs: ["get", "list", "watch", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: opendb-operator
  namespace: opendb-system
subjects:
  - kind: ServiceAccount
    name: opendb-operator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: opendb-operator
```

- [ ] **Step 3: Add Services**

Create `deploy/k8s/base/services.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: opendb-peer
  namespace: opendb-system
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: opendb
  ports:
    - name: internal
      port: 7000
      targetPort: 7000
---
apiVersion: v1
kind: Service
metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
  ports:
    - name: pgwire
      port: 5432
      targetPort: 5432
```

- [ ] **Step 4: Add StatefulSet**

Create `deploy/k8s/base/statefulset.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: opendb
  namespace: opendb-system
spec:
  serviceName: opendb-peer
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: opendb
  template:
    metadata:
      labels:
        app.kubernetes.io/name: opendb
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: opendb-node
          image: opendb-node:dev
          imagePullPolicy: IfNotPresent
          args:
            - "--node-id=$(OPENDB_ORDINAL)"
          env:
            - name: OPENDB_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb
          ports:
            - name: pgwire
              containerPort: 5432
            - name: health
              containerPort: 8080
            - name: internal
              containerPort: 7000
          readinessProbe:
            httpGet:
              path: /ready
              port: health
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /live
              port: health
            initialDelaySeconds: 10
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: /var/lib/opendb
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: 1Gi
```

- [ ] **Step 5: Add manifest checker**

Create `tools/check-manifests.ts`:

```ts
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import YAML from "yaml";

const baseDir = "deploy/k8s/base";
const docs = readdirSync(baseDir)
  .filter((file) => file.endsWith(".yaml"))
  .flatMap((file) => YAML.parseAllDocuments(readFileSync(join(baseDir, file), "utf8")).map((doc) => doc.toJSON()));

function requireDoc(kind: string, name: string): void {
  const found = docs.some((doc) => doc?.kind === kind && doc?.metadata?.name === name);
  if (!found) throw new Error(`Missing ${kind}/${name}`);
}

requireDoc("OpenDbCluster", "opendb");
requireDoc("Service", "opendb-peer");
requireDoc("Service", "opendb-pgwire");
requireDoc("StatefulSet", "opendb");

const statefulSet = docs.find((doc) => doc?.kind === "StatefulSet" && doc?.metadata?.name === "opendb");
if (statefulSet?.spec?.replicas !== 3) {
  throw new Error("StatefulSet must start with 3 replicas");
}

const claim = statefulSet?.spec?.volumeClaimTemplates?.[0]?.spec;
if (claim?.storageClassName !== "local-path") {
  throw new Error("Milestone 1 must use k3s local-path storage");
}

console.log("Kubernetes manifests passed static checks.");
```

- [ ] **Step 6: Add Vitest manifest test**

Create `tests/cluster/manifests.test.ts`:

```ts
import { execFileSync } from "node:child_process";
import { describe, expect, it } from "vitest";

describe("k3s manifests", () => {
  it("pass static manifest checks", () => {
    const output = execFileSync("npm", ["run", "check:manifests"], { encoding: "utf8" });
    expect(output).toContain("Kubernetes manifests passed static checks.");
  });
});
```

- [ ] **Step 7: Run manifest tests**

Run:

```bash
npm run check:manifests
npm run test:cluster
```

Expected:

```text
Kubernetes manifests passed static checks.
```

- [ ] **Step 8: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add deploy/k8s/base tools/check-manifests.ts tests/cluster/manifests.test.ts
git --git-dir=.opendb-git --work-tree=. commit -m "feat: add k3s deployment manifests"
```

---

### Task 10: SQL Parity Smoke Tests

**Files:**
- Create: `tests/parity/sql-smoke.test.ts`

- [ ] **Step 1: Add a TypeScript parity test**

Create `tests/parity/sql-smoke.test.ts`:

```ts
import { execFileSync, spawn } from "node:child_process";
import { describe, expect, it } from "vitest";

describe("opendb-node pgwire smoke", () => {
  it("serves create insert select through pgwire", async () => {
    execFileSync("cargo", ["build", "-p", "opendb-node"], { stdio: "inherit" });
    const child = spawn("cargo", [
      "run",
      "-p",
      "opendb-node",
      "--",
      "--node-id",
      "1",
      "--pgwire-addr",
      "127.0.0.1:55432",
      "--health-addr",
      "127.0.0.1:58080"
    ], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, RUST_LOG: "info" }
    });

    try {
      await new Promise((resolve) => setTimeout(resolve, 1500));
      const output = execFileSync("npm", ["exec", "tsx", "tools/pgwire-smoke.ts"], {
        encoding: "utf8",
        env: { ...process.env, OPENDB_PGWIRE_PORT: "55432" }
      });
      expect(output).toContain("pgwire smoke passed");
    } finally {
      child.kill("SIGTERM");
    }
  });
});
```

- [ ] **Step 2: Run parity tests**

Run:

```bash
npm run test:parity
```

Expected: PASS with `pgwire smoke passed`.

- [ ] **Step 3: Commit**

Run:

```bash
git --git-dir=.opendb-git --work-tree=. add tests/parity/sql-smoke.test.ts
git --git-dir=.opendb-git --work-tree=. commit -m "test: add pgwire sql smoke parity"
```

---

### Task 11: Final Milestone Verification

**Files:**
- No new files unless a previous task exposed a concrete fix.

- [ ] **Step 1: Run all Rust checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: all PASS.

- [ ] **Step 2: Run all TypeScript checks**

Run:

```bash
npm run check:no-python
npm run check:manifests
npm test
```

Expected: all PASS.

- [ ] **Step 3: Optional k3s UAT**

If k3s is available locally, run:

```bash
kubectl create namespace opendb-system
cargo run -p opendb-operator -- print-crd | kubectl apply -f -
kubectl apply -f deploy/k8s/base/
kubectl -n opendb-system get statefulset opendb
kubectl -n opendb-system get pvc
```

Expected:

```text
statefulset.apps/opendb
```

and three PVCs for the StatefulSet after pods are scheduled.

- [ ] **Step 4: Commit any verification fixes**

If verification required code changes, run:

```bash
git --git-dir=.opendb-git --work-tree=. add <changed-files>
git --git-dir=.opendb-git --work-tree=. commit -m "fix: stabilize milestone 1 verification"
```

If there were no changes, do not create an empty commit.

---

## Definition Of Done

Milestone 1 is done when:

- `cargo test --workspace` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `npm run check:no-python` passes.
- `npm run check:manifests` passes.
- `npm run test:parity` passes.
- `opendb-node` can serve the pgwire smoke path locally.
- root-range writes flow through the `opendb-consensus` boundary before they are durable.
- k3s manifests describe a 3-replica StatefulSet using `local-path` PVCs.
- `OpenDbCluster` CRD can be generated from Rust.

## Known Follow-Ups After Milestone 1

- Replace minimal pgwire coverage with broader protocol compatibility tests.
- Expand OpenRaft integration from the root range to multi-range membership, snapshots, and placement.
- Make `opendb-node` derive numeric node IDs from StatefulSet ordinal names.
- Add readiness checks tied to root-range leadership and replication state.
- Add MinIO-backed archival proof only after the core root-range path is stable.
- Add range metadata schema for Milestone 2.
