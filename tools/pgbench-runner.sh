#!/usr/bin/env bash
# Phase A: run pgbench against opendb-node (and optionally PG 16-alpine
# for the same-host comparison). Surfaces parser / protocol gaps so we
# can prioritize the Database-Mutex / MVCC / WAL-writer phases against
# real numbers.
#
# Env vars:
#   OPENDB_PORT       (default 15432)  -- pgwire port for opendb-node
#   PG_PORT           (default 15433)  -- pgwire port for PG container
#   SCALE             (default 1)      -- pgbench -s scale
#   CLIENTS           (default 1)      -- pgbench -c clients
#   THREADS           (default 1)      -- pgbench -j threads
#   DURATION          (default 20)     -- pgbench -T seconds
#   MODE              (default simple) -- pgbench -M simple|extended|prepared
#   SKIP_PG           (default 0)      -- if 1, only bench opendb
#   INIT_STEPS        (default dtGvp)  -- pgbench -I steps (G uses INSERT
#                                       instead of COPY which opendb has
#                                       not implemented yet)
#
# Output: writes opendb / pg run logs into docs/bench/, plus an
# overall report file.
set -euo pipefail
cd "$(dirname "$0")/.."

OPENDB_PORT="${OPENDB_PORT:-15432}"
PG_PORT="${PG_PORT:-15433}"
SCALE="${SCALE:-1}"
CLIENTS="${CLIENTS:-1}"
THREADS="${THREADS:-1}"
DURATION="${DURATION:-20}"
MODE="${MODE:-simple}"
SKIP_PG="${SKIP_PG:-0}"
INIT_STEPS="${INIT_STEPS:-dtGvp}"

DATE_STAMP="$(date +%Y-%m-%d)"
REPORT_DIR="docs/bench"
mkdir -p "$REPORT_DIR" .worktrees/.tmp-claude
OPENDB_LOG="$REPORT_DIR/pgbench-${DATE_STAMP}-opendb-c${CLIENTS}.log"
OPENDB_INIT_LOG="$REPORT_DIR/pgbench-${DATE_STAMP}-opendb-init.log"
PG_LOG="$REPORT_DIR/pgbench-${DATE_STAMP}-pg-c${CLIENTS}.log"
PG_INIT_LOG="$REPORT_DIR/pgbench-${DATE_STAMP}-pg-init.log"

cleanup() {
  set +e
  if [[ -n "${OPENDB_PID:-}" ]]; then
    kill -SIGTERM "$OPENDB_PID" 2>/dev/null
    sleep 1
    kill -SIGKILL "$OPENDB_PID" 2>/dev/null
  fi
  if [[ -n "${PG_CONTAINER:-}" ]]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [[ -n "${OPENDB_DATA:-}" && -d "$OPENDB_DATA" ]]; then
    rm -rf "$OPENDB_DATA"
  fi
}
trap cleanup EXIT

# ------- start opendb-node -------
OPENDB_DATA="$(mktemp -d -p .worktrees/.tmp-claude pgbench-XXXXXX)"
echo "[opendb] starting on $OPENDB_PORT, data=$OPENDB_DATA"
target/release/opendb-node \
  --node-id 1 \
  --data-dir "$OPENDB_DATA" \
  --pgwire-addr "127.0.0.1:$OPENDB_PORT" \
  --health-addr "127.0.0.1:$((OPENDB_PORT + 1))" \
  --admin-addr "127.0.0.1:$((OPENDB_PORT + 2))" \
  --internal-addr "127.0.0.1:$((OPENDB_PORT + 3))" \
  --advertise-addr "127.0.0.1:$((OPENDB_PORT + 3))" \
  >/dev/null 2>&1 &
OPENDB_PID=$!

# Wait for pgwire listener
for _ in {1..30}; do
  if (echo > /dev/tcp/127.0.0.1/$OPENDB_PORT) 2>/dev/null; then
    break
  fi
  sleep 0.2
done
echo "[opendb] pid=$OPENDB_PID port=$OPENDB_PORT ready"

# ------- pgbench init against opendb -------
echo "[opendb] running pgbench -i -s $SCALE ..."
set +e
docker run --rm --network host postgres:16-alpine \
  pgbench -h 127.0.0.1 -p "$OPENDB_PORT" -U opendb -d postgres -i -I "$INIT_STEPS" -s "$SCALE" \
  > "$OPENDB_INIT_LOG" 2>&1
OPENDB_INIT_RC=$?
set -e
echo "[opendb] pgbench -i rc=$OPENDB_INIT_RC (log: $OPENDB_INIT_LOG)"

if [[ "$OPENDB_INIT_RC" -eq 0 ]]; then
  echo "[opendb] running pgbench -c $CLIENTS -j $THREADS -T $DURATION -M $MODE ..."
  set +e
  docker run --rm --network host postgres:16-alpine \
    pgbench -h 127.0.0.1 -p "$OPENDB_PORT" -U opendb -d postgres \
    -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -M "$MODE" \
    > "$OPENDB_LOG" 2>&1
  OPENDB_BENCH_RC=$?
  set -e
  echo "[opendb] pgbench bench rc=$OPENDB_BENCH_RC (log: $OPENDB_LOG)"
else
  echo "[opendb] init failed, skipping bench"
  OPENDB_BENCH_RC=-1
fi

kill -SIGTERM "$OPENDB_PID" 2>/dev/null || true
wait "$OPENDB_PID" 2>/dev/null || true
unset OPENDB_PID
rm -rf "$OPENDB_DATA"
unset OPENDB_DATA

# ------- start PG 16-alpine -------
if [[ "$SKIP_PG" != "1" ]]; then
  PG_CONTAINER="opendb-pgbench-pg-$$"
  echo "[pg] starting postgres:16-alpine on $PG_PORT (container=$PG_CONTAINER)"
  docker run -d --rm --name "$PG_CONTAINER" \
    -p "$PG_PORT":5432 -e POSTGRES_PASSWORD=bench -e POSTGRES_USER=opendb \
    postgres:16-alpine \
    -c synchronous_commit=on >/dev/null

  for _ in {1..30}; do
    if docker exec "$PG_CONTAINER" pg_isready -q -U opendb 2>/dev/null; then
      break
    fi
    sleep 0.5
  done
  echo "[pg] ready"

  echo "[pg] running pgbench -i -s $SCALE ..."
  set +e
  docker run --rm --network host -e PGPASSWORD=bench postgres:16-alpine \
    pgbench -h 127.0.0.1 -p "$PG_PORT" -U opendb -d postgres -i -s "$SCALE" \
    > "$PG_INIT_LOG" 2>&1
  PG_INIT_RC=$?
  set -e

  if [[ "$PG_INIT_RC" -eq 0 ]]; then
    echo "[pg] running pgbench -c $CLIENTS -j $THREADS -T $DURATION -M $MODE ..."
    set +e
    docker run --rm --network host -e PGPASSWORD=bench postgres:16-alpine \
      pgbench -h 127.0.0.1 -p "$PG_PORT" -U opendb -d postgres \
      -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -M "$MODE" \
      > "$PG_LOG" 2>&1
    PG_BENCH_RC=$?
    set -e
    echo "[pg] pgbench bench rc=$PG_BENCH_RC (log: $PG_LOG)"
  else
    PG_BENCH_RC=-1
  fi

  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
  unset PG_CONTAINER
fi

# ------- summary line -------
echo
echo "================ Phase A summary ================"
echo "Scale=$SCALE  Clients=$CLIENTS  Threads=$THREADS  Duration=${DURATION}s  Mode=$MODE"
echo "OpenDB init rc: $OPENDB_INIT_RC"
echo "OpenDB bench rc: $OPENDB_BENCH_RC"
if [[ "$SKIP_PG" != "1" ]]; then
  echo "PG     init rc: ${PG_INIT_RC:-skipped}"
  echo "PG     bench rc: ${PG_BENCH_RC:-skipped}"
fi
echo "Logs in $REPORT_DIR"
