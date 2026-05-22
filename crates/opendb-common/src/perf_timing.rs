//! Lightweight env-gated perf counters used to attribute hot-path cost to
//! named spans. Designed for short-lived benchmark runs (sentropic POC seed)
//! where we want to know "where do the milliseconds go" without pulling in
//! a full flamegraph toolchain.
//!
//! Enable by setting `OPENDB_PERF_TIMING=1` in the process environment. When
//! the env var is not set, `Span::start` is a single atomic load and the
//! recording calls are no-ops, so the overhead in normal builds is
//! negligible.
//!
//! Counters are registered once via `register_counter` and looked up by
//! identity (pointer-equality on the leaked `&'static PerfCounter`). The
//! benchmark process dumps accumulated totals to stderr via
//! `dump_perf_counters_to_stderr`, called periodically inside `wal::append`
//! once the env var is set.

use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

pub struct PerfCounter {
    name: &'static str,
    total_ns: AtomicU64,
    calls: AtomicU64,
}

impl PerfCounter {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            total_ns: AtomicU64::new(0),
            calls: AtomicU64::new(0),
        }
    }

    pub fn record(&self, elapsed: Duration) {
        if !perf_enabled() {
            return;
        }
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.total_ns.fetch_add(ns, Ordering::Relaxed);
        self.calls.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.total_ns.load(Ordering::Relaxed),
            self.calls.load(Ordering::Relaxed),
        )
    }
}

static PERF_REGISTRY: Mutex<Vec<&'static PerfCounter>> = Mutex::new(Vec::new());

pub fn register_counter(counter: &'static PerfCounter) {
    let mut registry = PERF_REGISTRY.lock().expect("perf registry poisoned");
    if !registry
        .iter()
        .any(|existing| std::ptr::eq(*existing, counter))
    {
        registry.push(counter);
    }
}

pub fn perf_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("OPENDB_PERF_TIMING").is_ok())
}

pub struct Span {
    counter: &'static PerfCounter,
    start: Instant,
    enabled: bool,
}

impl Span {
    pub fn start(counter: &'static PerfCounter) -> Self {
        let enabled = perf_enabled();
        if enabled {
            register_counter(counter);
        }
        Self {
            counter,
            start: Instant::now(),
            enabled,
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if self.enabled {
            self.counter.record(self.start.elapsed());
        }
    }
}

pub fn dump_perf_counters_to_stderr() {
    if !perf_enabled() {
        return;
    }
    let registry = PERF_REGISTRY.lock().expect("perf registry poisoned");
    for counter in registry.iter() {
        let (total_ns, calls) = counter.snapshot();
        let total_ms = total_ns as f64 / 1_000_000.0;
        let mean_us = if calls > 0 {
            (total_ns as f64 / calls as f64) / 1_000.0
        } else {
            0.0
        };
        eprintln!(
            "OPENDB_PERF span={} total_ms={:.3} calls={} mean_us={:.3}",
            counter.name, total_ms, calls, mean_us
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perf_counter_records_when_enabled() {
        // Force-enable for this test by writing to the OnceLock bypass via a fresh counter.
        // We can't toggle the global env at test time without races, so instead we just
        // verify the Drop+record path on a counter when perf is enabled at process start.
        if !perf_enabled() {
            return;
        }
        static C: PerfCounter = PerfCounter::new("test.counter");
        {
            let _span = Span::start(&C);
            std::thread::sleep(Duration::from_micros(50));
        }
        let (ns, calls) = C.snapshot();
        assert!(calls >= 1);
        assert!(ns >= 50_000);
    }

    #[test]
    fn registry_dedupes_same_pointer() {
        static C: PerfCounter = PerfCounter::new("dedupe.test");
        register_counter(&C);
        register_counter(&C);
        let registry = PERF_REGISTRY.lock().unwrap();
        let same_count = registry.iter().filter(|c| std::ptr::eq(**c, &C)).count();
        assert_eq!(same_count, 1);
    }
}
