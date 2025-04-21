import os

filepath = 'benches/cpp_like_bench.rs'

with open(filepath, 'r') as f:
    text = f.read()

# 1. Update Exchange struct
if 'target_ids: Vec<CachePadded<AtomicU64>>,' not in text:
    text = text.replace(
        "last_ids: Vec<Arc<CachePadded<AtomicU64>>>,",
        "last_ids: Vec<Arc<CachePadded<AtomicU64>>>,\n    target_ids: Vec<CachePadded<AtomicU64>>,"
    )

# 2. Update new()
if 'target_ids.push(CachePadded::new(AtomicU64::new(0)));' not in text:
    text = text.replace(
        "let mut last_ids = Vec::with_capacity(num_workers);",
        "let mut last_ids = Vec::with_capacity(num_workers);\n        let mut target_ids = Vec::with_capacity(num_workers);"
    )
    text = text.replace(
        "last_ids.push(Arc::new(CachePadded::new(AtomicU64::new(0))));",
        "last_ids.push(Arc::new(CachePadded::new(AtomicU64::new(0))));\n            target_ids.push(CachePadded::new(AtomicU64::new(0)));"
    )
    text = text.replace(
        "last_ids,",
        "last_ids,\n            target_ids,"
    )

# 3. Update reset()
if 'target.0.store(0, Ordering::Relaxed);' not in text:
    text = text.replace(
        "last.0.store(0, Ordering::Relaxed);\n        }",
        "last.0.store(0, Ordering::Relaxed);\n        }\n        for target in &self.target_ids {\n            target.0.store(0, Ordering::Relaxed);\n        }"
    )

# 4. Update Worker loop
text = text.replace(
"""                            Some(order) => {
                                last_id.0.store(order.id, Ordering::Relaxed);
                                worker.handle(order);
                            }""",
"""                            Some(order) => {
                                worker.handle(order);
                                last_id.0.fetch_add(1, Ordering::Release);
                            }"""
)

# 5. Update submit_order
text = text.replace(
"""    pub fn submit_order(&self, order: Order) -> usize {
        let idx = (order.symbol_id as usize) % self.worker_count;
        self.senders[idx].push(order);
        idx
    }""",
"""    pub fn submit_order(&self, order: Order) -> usize {
        let idx = (order.symbol_id as usize) % self.worker_count;
        self.target_ids[idx].0.fetch_add(1, Ordering::Relaxed);
        self.senders[idx].push(order);
        idx
    }"""
)

# 6. Update drain_local to drain
text = text.replace(
"""    pub fn drain_local(&self, local_targets: &[u64]) {
        for (idx, &target) in local_targets.iter().enumerate() {
            if target == 0 {
                continue;
            }
            while self.last_ids[idx].0.load(Ordering::Relaxed) < target {
                std::thread::yield_now();
            }
        }
    }""",
"""    pub fn drain(&self) {
        for i in 0..self.worker_count {
            let target = self.target_ids[i].0.load(Ordering::Acquire);
            if target == 0 { continue; }
            while self.last_ids[i].0.load(Ordering::Acquire) < target {
                std::hint::spin_loop();
            }
        }
    }"""
)

# 7. Update benchmark_worker
text = text.replace(
"""    let mut local_wait = Duration::from_nanos(0);
    // 【新增】本地化维护目标 ID
    let mut local_targets = vec![0u64; engine.worker_count];

    for _ in 0..iterations {
        for order in orders {
            if MEASURE_LATENCY.load(Ordering::Relaxed) {
                if let Some(times) = sub_times {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;
                    times[order.id as usize].store(now, Ordering::Relaxed);
                }
            }
            // 提交订单并记录投递到了哪个 worker
            let worker_idx = engine.submit_order(*order);
            local_targets[worker_idx] = order.id;
        }
    }

    let d_start = Instant::now();
    // 使用局部 drain 等待自己提交的任务完成
    engine.drain_local(&local_targets);
    local_wait += d_start.elapsed();""",
"""    let mut local_wait = Duration::from_nanos(0);

    for _ in 0..iterations {
        for order in orders {
            if MEASURE_LATENCY.load(Ordering::Relaxed) {
                if let Some(times) = sub_times {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;
                    times[order.id as usize].store(now, Ordering::Relaxed);
                }
            }
            engine.submit_order(*order);
        }
    }

    let d_start = Instant::now();
    engine.drain();
    local_wait += d_start.elapsed();"""
)

# warmup drain
text = text.replace(
"""                let mut local_targets = vec![0u64; num_threads];

                for j in 0..200_000 {
                    let warmup_sym_id = rng.random_range(0..10) as u64; // 随机派发
                    let o = Order {
                        id: j as u64,
                        client_order_id: 0,
                        symbol_id: warmup_sym_id,
                        side: Side::Buy,
                        ty: OrderType::Limit,
                        price: Price(std::num::NonZeroU64::new(100)),
                        quantity: 1,
                        active: true,
                    };
                    let w_idx = e.submit_order(o);
                    local_targets[w_idx] = o.id;
                }
                e.drain_local(&local_targets);""",
"""                for j in 0..200_000 {
                    let warmup_sym_id = rng.random_range(0..10) as u64; // 随机派发
                    let o = Order {
                        id: j as u64,
                        client_order_id: 0,
                        symbol_id: warmup_sym_id,
                        side: Side::Buy,
                        ty: OrderType::Limit,
                        price: Price(std::num::NonZeroU64::new(100)),
                        quantity: 1,
                        active: true,
                    };
                    e.submit_order(o);
                }
                e.drain();"""
)

with open(filepath, 'w') as f:
    f.write(text)

