//! Criterion benchmarks for OrderWorker (multi-threaded matching engine).
//!
//! Run with: `cargo bench`
//! Generate HTML reports: `cargo bench -- --html`

use criterion::{
    criterion_group, criterion_main, Criterion,
    SamplingMode, Throughput,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use rs_matching_engine::broadcast;
use rs_matching_engine::mpmc::channel;
use rs_matching_engine::mpmc::Consumer;
use rs_matching_engine::mpmc::Producer;
use rs_matching_engine::worker::Worker;
use rs_matching_engine::order::{Order, OrderType, Price, Side};
use rs_matching_engine::order_book::FlatOrderBook;
use rs_matching_engine::worker::OrderWorker;

// Submission instant_ns keyed by order_id (for latency measurement)
// Must use LazyLock because DashMap::new() is not const
use dashmap::DashMap;
use std::sync::LazyLock;
static SUBMISSION_TS: LazyLock<DashMap<u64, u64>, fn() -> DashMap<u64, u64>> =
    LazyLock::new(DashMap::new);

// ============================================================
// Order generation
// ============================================================

fn make_order(id: u64, price: u64, qty: u64, side: Side, symbol_id: u64) -> Order {
    Order {
        id,
        price: Price(std::num::NonZeroU64::new(price)),
        client_order_id: 0,
        symbol_id,
        quantity: qty,
        side,
        ty: OrderType::Limit,
        active: true,
    }
}

fn generate_orders(count: usize, num_symbols: usize) -> Vec<Order> {
    (0..count as u64)
        .map(|i| {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            let symbol_id = (i % num_symbols as u64) + 1;
            let price = 1500 + (i % 500);
            make_order(i + 1, price, 10, side, symbol_id)
        })
        .collect()
}

// ============================================================
// BenchmarkEngine: manages worker threads + MPMC channels
// ============================================================

pub struct BenchmarkEngine {
    senders: Vec<rs_matching_engine::mpmc::Sender<Order>>,
    worker_count: usize,
    // Per-worker: the highest order ID this worker has processed
    last_ids: Vec<Arc<AtomicU64>>,
    // Per-worker: the highest order ID submitted to this specific worker
    target_ids: Vec<AtomicU64>,
    _handles: Vec<JoinHandle<()>>,
    // Trade broadcast receiver — stored for latency measurement
    _trade_rx: broadcast::Receiver<Vec<rs_matching_engine::order_book::Trade>>,
}

impl BenchmarkEngine {
    /// Create a new engine with `num_workers` worker threads.
    /// Workers are pinned to the first `num_workers` CPU cores via core_affinity.
    pub fn new(num_workers: usize, channel_cap: usize) -> Self {
        let (trade_tx, trade_rx) = broadcast::channel(1024);
        let mut last_ids = Vec::with_capacity(num_workers);
        let mut senders = Vec::with_capacity(num_workers);
        let mut handles = Vec::new();

        let mut target_ids = Vec::with_capacity(num_workers);

        let cores = core_affinity::get_core_ids().unwrap();

        for worker_idx in 0..num_workers {
            last_ids.push(Arc::new(AtomicU64::new(0)));
            target_ids.push(AtomicU64::new(0));
            let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
            let (tx, rx) = channel::<Order>(channel_cap);
            let trade_tx_clone = trade_tx.clone();
            let last_id = last_ids[worker_idx].clone();
            // Clone cores so each worker gets its own copy (cores is moved into closure)
            let cores_clone = cores.clone();

            let handle = std::thread::Builder::new()
                .name(format!("bench-worker-{worker_idx}"))
                .spawn(move || {
                    // Pin worker to its assigned core
                    if let Some(&cid) = cores_clone.get(worker_idx) {
                        core_affinity::set_for_current(cid);
                    }

                    let mut worker = OrderWorker::new(book, trade_tx_clone);

                    loop {
                        match rx.take() {
                            Some(order) => {
                                last_id.store(order.id, Ordering::Relaxed);
                                worker.handle(order);
                            }
                            None => std::hint::spin_loop(),
                        }
                    }
                })
                .unwrap();

            senders.push(tx);
            handles.push(handle);
        }

        Self {
            senders,
            worker_count: num_workers,
            last_ids,
            target_ids,
            _handles: handles,
            _trade_rx: trade_rx,
        }
    }

    /// Submit a single order, routing to the correct worker by symbol_id.
    /// Records submission timestamp for latency measurement.
    pub fn submit(&self, order: Order) {
        let ts = Instant::now().elapsed().as_nanos() as u64;
        SUBMISSION_TS.insert(order.id, ts);
        let idx = (order.symbol_id as usize) % self.worker_count;
        self.target_ids[idx].store(order.id, Ordering::Relaxed);
        self.senders[idx].push(order);
    }

    /// Submit a batch of orders
    pub fn submit_batch(&self, orders: &[Order]) {
        for &order in orders {
            self.submit(order);
        }
    }

    /// spin until every worker has processed its targeted last order.
    pub fn wait_for_completion(&self) {
        for (idx, target_id) in self.target_ids.iter().enumerate() {
            let target = target_id.load(Ordering::Relaxed);
            if target == 0 { continue; } // No orders routed to this worker

            while self.last_ids[idx].load(Ordering::Relaxed) < target {
                std::hint::spin_loop();
            }
        }
    }

    /// Re-subscribe to the trade broadcast channel for latency collection.
    /// Returns a fresh Receiver; workers send to the same broadcast channel.
    /// `resubscribe()` takes `&self` (immutable), so it works through a `Mutex` guard.
    pub fn take_trade_rx(&self) -> broadcast::Receiver<Vec<rs_matching_engine::order_book::Trade>> {
        self._trade_rx.resubscribe()
    }
}

// ============================================================
// Benchmark: throughput (single symbol)
// ============================================================

fn bench_throughput(c: &mut Criterion) {
    let orders = generate_orders(1_000_000, 1);

    let mut group = c.benchmark_group("throughput");
    // Flat sampling is better for long-running benchmarks (seconds per iteration)
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);
    group.throughput(Throughput::Elements(orders.len() as u64));

    // Create engine ONCE outside iter_custom to avoid per-sample thread-spawn overhead
    // Use capacity > 1_000_000 to prevent channel full panics, as all are routed to the same worker here
    let engine = BenchmarkEngine::new(4, 2_097_152);

    group.bench_function("1M_single_symbol", |b| {
        b.iter_custom(|iters| {
            let mut total_ns = 0i128;
            for _ in 0..iters {
                let start = Instant::now();
                engine.submit_batch(&orders);
                engine.wait_for_completion();
                total_ns += start.elapsed().as_nanos() as i128;
            }
            // Return average wall time per "run"
            std::time::Duration::from_nanos((total_ns / iters.max(1) as i128) as u64)
        });
    });

    group.finish();
}

// ============================================================
// Benchmark: throughput (multi-symbol, sharded across workers)
// ============================================================

fn bench_throughput_multi_symbol(c: &mut Criterion) {
    let orders = generate_orders(500_000, 4); // 4 symbols → routes to 4 workers

    let mut group = c.benchmark_group("throughput");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);
    group.throughput(Throughput::Elements(orders.len() as u64));

    // Capacity must be > 500_000 
    let engine = BenchmarkEngine::new(4, 1_048_576);

    group.bench_function("500K_4symbols_4workers", |b| {
        b.iter_custom(|iters| {
            let mut total_ns = 0i128;
            for _ in 0..iters {
                let start = Instant::now();
                engine.submit_batch(&orders);
                engine.wait_for_completion();
                total_ns += start.elapsed().as_nanos() as i128;
            }
            std::time::Duration::from_nanos((total_ns / iters.max(1) as i128) as u64)
        });
    });

    group.finish();
}

// ============================================================
// Benchmark: latency (P50/P99)
// ============================================================

fn bench_latency(c: &mut Criterion) {
    let orders = generate_orders(100_000, 1);

    let mut group = c.benchmark_group("latency");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    // Arc<Mutex<>> lets us call take_trade_rx (requires &mut) inside the iter_custom closure
    // Capacity must be > 100_000
    let engine: Arc<Mutex<BenchmarkEngine>> =
        Arc::new(Mutex::new(BenchmarkEngine::new(4, 1_048_576)));

    group.bench_function("100K_orders", |b| {
        b.iter_custom(|iters| {
            let mut all_latencies: Vec<u64> = Vec::with_capacity(100_000 * iters as usize);

            for _ in 0..iters {
                SUBMISSION_TS.clear();

                // Re-subscribe to trade broadcast BEFORE submission so we catch all trades
                let mut trade_rx = {
                    let eng = engine.lock().unwrap();
                    eng.take_trade_rx()
                };

                // Collector thread: reads trades, records completion instant_ns per taker_order_id
                let (tx, rx) = std::sync::mpsc::channel::<(u64, u64)>();
                let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let stop_flag_clone = stop_flag.clone();
                let collector = std::thread::spawn(move || {
                    while !stop_flag_clone.load(Ordering::Relaxed) {
                        if let Ok(trades) = trade_rx.try_recv() {
                            let now = Instant::now().elapsed().as_nanos() as u64;
                            for trade in trades {
                                let _ = tx.send((trade.taker_order_id, now));
                            }
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    // Drain the remaining to avoid missing the very last trades
                    while let Ok(trades) = trade_rx.try_recv() {
                        let now = Instant::now().elapsed().as_nanos() as u64;
                        for trade in trades {
                            let _ = tx.send((trade.taker_order_id, now));
                        }
                    }
                });

                // Submit all orders (records submission timestamp per order via DashMap)
                {
                    let eng = engine.lock().unwrap();
                    eng.submit_batch(&orders);
                }

                // Wait for all orders to be processed
                {
                    let eng = engine.lock().unwrap();
                    eng.wait_for_completion();
                }

                // Signal and wait for collector
                stop_flag.store(true, Ordering::Relaxed);
                collector.join().unwrap();

                // Pair submission with completion and compute latency
                while let Ok((order_id, complete_ns)) = rx.try_recv() {
                    if let Some(submit_ns) = SUBMISSION_TS.get(&order_id) {
                        all_latencies.push(complete_ns.saturating_sub(*submit_ns));
                    }
                }
            }

            if !all_latencies.is_empty() {
                all_latencies.sort();
                let n = all_latencies.len();
                let p50 = all_latencies[n * 50 / 100];
                let p99 = all_latencies[n * 99 / 100];
                let max_lat = *all_latencies.last().unwrap();
                eprintln!(
                    "latency: count={} P50={}ns P99={}ns max={}ns",
                    n, p50, p99, max_lat
                );
            }

            // Dummy duration; wall time is not the primary metric here
            std::time::Duration::from_secs(1)
        });
    });

    group.finish();
}

// ============================================================
// Criterion entry point
// ============================================================

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_throughput, bench_throughput_multi_symbol, bench_latency
}
criterion_main!(benches);
