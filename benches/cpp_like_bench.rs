use crossbeam_utils::CachePadded;
use rand::{Rng,RngExt};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rs_matching_engine::broadcast;
use rs_matching_engine::mpmc::channel;
use rs_matching_engine::mpmc::Consumer;
use rs_matching_engine::mpmc::Producer;
use rs_matching_engine::worker::{Worker, OrderWorker};
use rs_matching_engine::order::{Order, OrderType, Price, Side};
use rs_matching_engine::order_book::{FlatOrderBook, Trade};

static MEASURE_LATENCY: AtomicBool = AtomicBool::new(false);
static LATENCY_INDEX: AtomicUsize = AtomicUsize::new(0);

struct Exchange {
    senders: Vec<rs_matching_engine::mpmc::Sender<Order>>,
    worker_count: usize,
    last_ids: Vec<Arc<CachePadded<AtomicU64>>>,
    target_ids: Vec<CachePadded<AtomicU64>>,
    _handles: Vec<JoinHandle<()>>,
    trade_rx: broadcast::Receiver<Vec<Trade>>,
}

impl Exchange {
    pub fn new(num_workers: usize) -> Self {
        let (trade_tx, trade_rx) = broadcast::channel(1024);
        let mut last_ids = Vec::with_capacity(num_workers);
        let mut senders = Vec::with_capacity(num_workers);
        let mut handles = Vec::new();
        let mut target_ids = Vec::with_capacity(num_workers);

        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let channel_cap = 8_388_608; 

        for worker_idx in 0..num_workers {
            last_ids.push(Arc::new(CachePadded::new(AtomicU64::new(0))));
            target_ids.push(CachePadded::new(AtomicU64::new(0)));
            let book = FlatOrderBook::new(1, 100, 100_000, 1, 1_000_000);
            let (tx, rx) = channel::<Order>(channel_cap);
            let trade_tx_clone = trade_tx.clone();
            let last_id = last_ids[worker_idx].clone();
            let cores_clone = cores.clone();

            let handle = thread::Builder::new()
                .name(format!("worker-{}", worker_idx))
                .spawn(move || {
                    if let Some(&cid) = cores_clone.get(worker_idx) {
                        core_affinity::set_for_current(cid);
                    }
                    let mut worker = OrderWorker::new(book, trade_tx_clone);
                    let mut batch = [Order::default(); 256];
                    loop {
                        let count = rx.take_batch(&mut batch);
                        if count > 0 {
                            for i in 0..count {
                                worker.handle(batch[i]);
                            }
                            last_id.fetch_add(count as u64, Ordering::Relaxed);
                        } else {
                            std::hint::spin_loop();
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
            trade_rx,
        }
    }

    pub fn submit_order(&self, order: Order, _thread_id: usize) {
        let idx = (order.symbol_id as usize) % self.worker_count;
        self.target_ids[idx].fetch_add(1, Ordering::Relaxed);
        self.senders[idx].push(order);
    }

    #[inline(always)]
    pub fn push_batch(&self, idx: usize, batch: &[Order]) {
        if batch.is_empty() { return; }
        self.target_ids[idx].fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.senders[idx].push_slice(batch);
    }

    pub fn drain(&self) {
        for (idx, target_id) in self.target_ids.iter().enumerate() {
            let target = target_id.load(Ordering::Relaxed);
            if target == 0 { continue; }
            while self.last_ids[idx].load(Ordering::Relaxed) < target {
                std::hint::spin_loop();
            }
        }
    }

    pub fn take_trade_rx(&self) -> broadcast::Receiver<Vec<Trade>> {
        self.trade_rx.resubscribe()
    }
}

fn pin_thread_with_offset(thread_id: usize) {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let num_cores = cores.len();
    if num_cores > 0 {
        let offset = num_cores / 2;
        let core_idx = (thread_id + offset) % num_cores;
        core_affinity::set_for_current(cores[core_idx]);
    }
}

fn benchmark_worker(
    engine: &Exchange,
    orders: &[Order],
    thread_id: usize,
    iterations: usize,
    sub_times: Option<&[AtomicI64]>,
    thread_wait: &mut Duration,
) {
    pin_thread_with_offset(thread_id);

    let local_wait = Duration::from_nanos(0);
    // Use an uninitialized or default slice of Vectors to cache local orders.
    // 256 is PRODUCER_BATCH_SIZE like C++
    let mut local_batches = vec![Vec::with_capacity(256); engine.worker_count];

    for _ in 0..iterations {
        for order in orders {
            if MEASURE_LATENCY.load(Ordering::Relaxed) {
                if let Some(times) = sub_times {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
                    times[order.id as usize].store(now, Ordering::Relaxed);
                }
            }
            
            let idx = (order.symbol_id as usize) % engine.worker_count;
            local_batches[idx].push(*order);

            if local_batches[idx].len() >= 256 {
                engine.push_batch(idx, &local_batches[idx]);
                local_batches[idx].clear();
            }
        }
    }
    
    // Flush remaining orders
    for i in 0..engine.worker_count {
        if !local_batches[i].is_empty() {
            engine.push_batch(i, &local_batches[i]);
            local_batches[i].clear();
        }
    }

    *thread_wait = local_wait;
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--latency" || a == "-l") {
        MEASURE_LATENCY.store(true, Ordering::Relaxed);
    }

    let total_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let mut num_threads = total_cores / 2;
    if num_threads < 1 { num_threads = 1; }

    let orders_per_thread: u64 = 10_000_000;
    let pool_size: u64 = 200_000;
    let iterations = orders_per_thread / pool_size;
    let total_orders = (num_threads as u64) * orders_per_thread;

    println!("Running Warmup Phase with {} threads...", num_threads);
    {
        let warmup_engine = Exchange::new(num_threads);
        let mut threads = Vec::new();
        
        let mut dummy = 0i64;
        for k in 0..100_000 { dummy += k; }
        black_box(dummy);

        for id in 0..num_threads {
            let warmup_eng_ptr = &warmup_engine as *const Exchange as usize; 
            threads.push(std::thread::spawn(move || {
                let e = unsafe { &*(warmup_eng_ptr as *const Exchange) };
                let warmup_sym_id = 0;
                for j in 0..200_000 {
                    let o = Order { id: j as u64, client_order_id: 0, symbol_id: warmup_sym_id, side: Side::Buy, ty: OrderType::Limit, price: Price(std::num::NonZeroU64::new(100)), quantity: 1, active: true };
                    e.submit_order(o, id);
                }
            }));
        }
        for t in threads { t.join().unwrap(); }
        warmup_engine.drain();
    }
    println!("Warmup complete.");

    println!("Preparing benchmark with {} threads...", num_threads);

    if MEASURE_LATENCY.load(Ordering::Relaxed) {
        println!("Latency measurement ENABLED (expect lower throughput).");
    } else {
        println!("Latency measurement DISABLED (max throughput).");
    }

    println!("Pre-generating orders...");

    let mut submission_times: Vec<AtomicI64> = Vec::new();
    let mut latencies: Vec<i64> = Vec::new();
    
    if MEASURE_LATENCY.load(Ordering::Relaxed) {
        submission_times.resize_with((total_orders + 1) as usize, || AtomicI64::new(0));
        latencies.resize(total_orders as usize, 0);
    }

    let mut thread_orders = vec![Vec::with_capacity(pool_size as usize); num_threads];

    let mut id_counter = 1u64;
    for i in 0..num_threads {
        let mut rng = rand::rng(); 
        for j in 0..pool_size {
            let side = if rng.random_bool(0.5) { Side::Buy } else { Side::Sell };
            let price = rng.random_range(10000..=10020);
            let qty = rng.random_range(1..=100);
            let symbol_id = (i % 10) as u64;

            let o = Order {
                id: (i as u64 * pool_size) + j + 1,
                client_order_id: 0,
                symbol_id,
                side,
                ty: OrderType::Limit,
                price: Price(std::num::NonZeroU64::new(price)),
                quantity: qty,
                active: true,
            };
            thread_orders[i].push(o);
        }
    }

    println!("Starting benchmark (10 runs)....");

    let mut throughputs = Vec::with_capacity(10);
    let mut durations = Vec::with_capacity(10);

    let engine = Arc::new(Exchange::new(num_threads));
    
    for run in 0..10 {

        
        let start = Instant::now();
        let total_trades = Arc::new(AtomicU64::new(0));
        
        LATENCY_INDEX.store(0, Ordering::Relaxed);
        
        let mut trade_rx = engine.take_trade_rx();
        let stop_collector = Arc::new(AtomicBool::new(false));
        let cloned_stop = stop_collector.clone();
        let cloned_trades = total_trades.clone();
        
        let sub_ptr = submission_times.as_ptr() as usize;
        let lat_ptr = latencies.as_mut_ptr() as usize;
        let max_lat_len = latencies.len();

        let collector = std::thread::spawn(move || {
            let sub_ptr = sub_ptr as *const AtomicI64;
            let lat_ptr = lat_ptr as *mut i64;
            while !cloned_stop.load(Ordering::Relaxed) {
                if let Ok(trades) = trade_rx.try_recv() {
                    cloned_trades.fetch_add(trades.len() as u64, Ordering::Relaxed);
                    if MEASURE_LATENCY.load(Ordering::Relaxed) {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
                        for trade in trades {
                            unsafe {
                                let sub_time = (*sub_ptr.add(trade.taker_order_id as usize)).load(Ordering::Relaxed);
                                if sub_time > 0 {
                                    let idx = LATENCY_INDEX.fetch_add(1, Ordering::Relaxed);
                                    if idx < max_lat_len {
                                        std::ptr::write(lat_ptr.add(idx), now - sub_time);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    std::thread::yield_now();
                }
            }
            while let Ok(trades) = trade_rx.try_recv() {
                cloned_trades.fetch_add(trades.len() as u64, Ordering::Relaxed);
            }
        });

        let mut workers = Vec::new();
        let thread_waits = Arc::new(Mutex::new(Vec::new()));

        for i in 0..num_threads {
            let thread_orders_ref = thread_orders[i].clone();
            let engine_ref = engine.clone();
            
            let sub_ref = if MEASURE_LATENCY.load(Ordering::Relaxed) { Some(&submission_times[..]) } else { None };
            let sub_ptr_safe = sub_ref.map(|s| s.as_ptr() as usize);
            let len = submission_times.len();
            let tw_arc = thread_waits.clone();

            workers.push(std::thread::spawn(move || {
                let reconstructed_sub = sub_ptr_safe.map(|ptr| unsafe { std::slice::from_raw_parts(ptr as *const AtomicI64, len) });
                let mut my_wait = Duration::from_nanos(0);
                benchmark_worker(&engine_ref, &thread_orders_ref, i, iterations as usize, reconstructed_sub, &mut my_wait);
                tw_arc.lock().unwrap().push(my_wait);
            }));
        }

        for w in workers { w.join().unwrap(); }
        engine.drain();
        
        stop_collector.store(true, Ordering::Relaxed);
        collector.join().unwrap();

        let diff = start.elapsed();
        let diff_secs = diff.as_secs_f64();
        let tput = (total_orders as f64 / diff_secs) as u64;

        let total_wait_sum: u128 = thread_waits.lock().unwrap().iter().map(|d| d.as_nanos()).sum();
        let avg_wait_ns = total_wait_sum as f64 / total_orders as f64;

        durations.push(diff_secs);
        throughputs.push(tput);

        println!("Run {}: {:.6} seconds. Throughput: {} orders/second", run + 1, diff_secs, tput);
        println!("  Trades Executed: {}", total_trades.load(Ordering::Relaxed));
        println!("  Backpressure (Wait): Total={}ns Avg={:.2}ns/order", total_wait_sum, avg_wait_ns);

        if MEASURE_LATENCY.load(Ordering::Relaxed) {
            let num_lats = std::cmp::min(LATENCY_INDEX.load(Ordering::Relaxed), latencies.len());
            if num_lats > 0 {
                let slice = &mut latencies[0..num_lats];
                slice.sort_unstable();
                let p50 = slice[(num_lats as f64 * 0.5) as usize];
                let p99 = slice[(num_lats as f64 * 0.99) as usize];
                let max_lat = slice[num_lats - 1];
                let sum: i64 = slice.iter().sum();
                let avg = sum / num_lats as i64;
                println!("  Latency (ns): Avg={} P50={} P99={} Max={}", avg, p50, p99, max_lat);
            } else {
                println!("  No trades recorded (latencies).");
            }
        }
    }

    let min_tput = *throughputs.iter().min().unwrap();
    let max_tput = *throughputs.iter().max().unwrap();
    let avg_tput: u64 = throughputs.iter().sum::<u64>() / throughputs.len() as u64;

    println!("\n--- Benchmark Summary (10 Runs) ---");
    println!("Average Throughput: {} orders/second", avg_tput);
    println!("Minimum Throughput: {} orders/second", min_tput);
    println!("Maximum Throughput: {} orders/second", max_tput);
}
