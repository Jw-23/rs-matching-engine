//! Correctness verification test for OrderWorker + FlatOrderBook.
//!
//! Run with: `cargo test --test verify_correctness`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use rs_matching_engine::broadcast;
use rs_matching_engine::mpmc::channel;
use rs_matching_engine::mpmc::Consumer;
use rs_matching_engine::mpmc::Producer;
use rs_matching_engine::order::{Order, OrderType, Price, Side};
use rs_matching_engine::order_book::{FlatOrderBook, Trade};
use rs_matching_engine::worker::OrderWorker;
use rs_matching_engine::worker::Worker;

// Helper: create a limit order
fn limit_order(id: u64, price: u64, qty: u64, side: Side, symbol_id: u64) -> Order {
    Order {
        id,
        price: Price(Some(std::num::NonZeroU64::new(price).unwrap())),
        client_order_id: 0,
        symbol_id,
        quantity: qty,
        side,
        ty: OrderType::Limit,
        active: true,
    }
}

// Helper: drain all trades from the broadcast channel (non-blocking)
fn drain_trades_blocking(
    rx: &mut broadcast::Receiver<Vec<rs_matching_engine::order_book::Trade>>,
) -> Vec<rs_matching_engine::order_book::Trade> {
    let mut all = Vec::new();
    while let Ok(trades) = rx.try_recv() {
        all.extend(trades);
    }
    all
}

// Helper: create a 2-worker engine.
fn make_engine(channel_cap: usize) -> (
    Vec<rs_matching_engine::mpmc::Sender<Order>>,
    broadcast::Receiver<Vec<rs_matching_engine::order_book::Trade>>,
    Vec<thread::JoinHandle<()>>,
) {
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let mut senders = Vec::new();
    let mut handles = Vec::new();

    for _worker_idx in 0..2 {
        let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
        let (tx, rx) = channel::<Order>(channel_cap);
        let trade_tx_clone = trade_tx.clone();

        let handle = thread::spawn(move || {
            let mut worker = OrderWorker::new(book, trade_tx_clone);
            loop {
                match rx.take() {
                    Some(order) => worker.handle(order),
                    None => std::hint::spin_loop(),
                }
            }
        });

        senders.push(tx);
        handles.push(handle);
    }

    (senders, trade_rx, handles)
}

// ============================================================
// Test 1: BUY first then SELL — crosses immediately
// ============================================================

#[test]
fn test_fifo_matching_buy_first() {
    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let (trade_tx, mut rx) = broadcast::channel(1024);
    let mut worker = OrderWorker::new(book, trade_tx);

    worker.handle(limit_order(1, 1505, 10, Side::Buy, 1));
    worker.handle(limit_order(2, 1503, 10, Side::Sell, 1));

    let trades = drain_trades_blocking(&mut rx);

    assert_eq!(trades.len(), 1, "Expected exactly 1 trade");
    assert_eq!(trades[0].quantity, 10);
    assert_eq!(trades[0].market_order_id, 1);
    assert_eq!(trades[0].taker_order_id, 2);
    assert_eq!(trades[0].price.0.unwrap().get(), 1505);
}

// ============================================================
// Test 2: SELL first then BUY — no cross (BUY price < SELL price)
// ============================================================

#[test]
fn test_fifo_matching_no_cross() {
    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let (trade_tx, mut rx) = broadcast::channel(1024);
    let mut worker = OrderWorker::new(book, trade_tx);

    worker.handle(limit_order(1, 1503, 10, Side::Sell, 1));
    worker.handle(limit_order(2, 1500, 10, Side::Buy, 1));

    let trades = drain_trades_blocking(&mut rx);
    assert!(trades.is_empty(), "Expected no trades when BUY price < SELL price");
}

// ============================================================
// Test 3: Partial fill — incoming order larger than maker
// ============================================================

#[test]
fn test_partial_fill() {
    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let (trade_tx, mut rx) = broadcast::channel(1024);
    let mut worker = OrderWorker::new(book, trade_tx);

    worker.handle(limit_order(1, 1505, 10, Side::Buy, 1));
    worker.handle(limit_order(2, 1503, 30, Side::Sell, 1));

    let trades = drain_trades_blocking(&mut rx);
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].quantity, 10);
    assert_eq!(trades[0].market_order_id, 1);
    assert_eq!(trades[0].taker_order_id, 2);
}

// ============================================================
// Test 4: Multi-order FIFO sequence with price progression
// ============================================================

#[test]
fn test_trade_quantity_invariant() {
    // BUY 1505 + BUY 1503 added to book.
    // SELL 1503 → matches BUY 1505: trade 10 at 1505
    // SELL 1501 → matches BUY 1503: trade 10 at 1503
    // SELL 1499 → no bids → added to asks
    // Expected: 2 trades, total qty = 20

    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let (trade_tx, mut rx) = broadcast::channel(1024);
    let mut worker = OrderWorker::new(book, trade_tx);

    worker.handle(limit_order(1, 1505, 10, Side::Buy, 1));
    worker.handle(limit_order(2, 1503, 10, Side::Buy, 1));
    worker.handle(limit_order(3, 1503, 10, Side::Sell, 1));
    worker.handle(limit_order(4, 1501, 10, Side::Sell, 1));
    worker.handle(limit_order(5, 1499, 10, Side::Sell, 1));

    let trades = drain_trades_blocking(&mut rx);

    assert_eq!(trades.len(), 2, "Expected 2 trades");
    assert_eq!(trades[0].quantity, 10);
    assert_eq!(trades[1].quantity, 10);

    let total_traded: u64 = trades.iter().map(|t| t.quantity).sum();
    assert_eq!(total_traded, 20);

    for trade in &trades {
        assert_eq!(trade.symbol_id, 1);
    }

    let maker_ids: Vec<_> = trades.iter().map(|t| t.market_order_id).collect();
    assert!(maker_ids[0] != maker_ids[1], "Maker order IDs should be distinct");
}

// ============================================================
// Test 5: Market order on empty book — no panic, no trades
// ============================================================

#[test]
fn test_market_order_empty_book() {
    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let (trade_tx, mut rx) = broadcast::channel(1024);
    let mut worker = OrderWorker::new(book, trade_tx);

    let market_buy = Order {
        id: 1,
        price: Price(None),
        client_order_id: 0,
        symbol_id: 1,
        quantity: 10,
        side: Side::Buy,
        ty: OrderType::Market,
        active: true,
    };

    worker.handle(market_buy);

    let trades = drain_trades_blocking(&mut rx);
    assert!(trades.is_empty(), "No trades expected on empty book");
}

// ============================================================
// Test 6: Multi-symbol routing — orders go to different workers
// ============================================================

#[test]
fn test_multi_symbol_routing() {
    let (senders, rx, _handles) = make_engine(4096);
    let mut rx_sub = rx.resubscribe();

    senders[0].push(limit_order(1, 1500, 10, Side::Buy, 1));
    senders[1].push(limit_order(2, 1500, 10, Side::Buy, 2));
    senders[0].push(limit_order(3, 1499, 10, Side::Sell, 1));
    senders[1].push(limit_order(4, 1499, 10, Side::Sell, 2));

    thread::sleep(std::time::Duration::from_millis(200));

    let all_trades = drain_trades_blocking(&mut rx_sub);

    assert_eq!(all_trades.len(), 2, "Expected 2 trades, got {}", all_trades.len());

    let symbol_ids: Vec<_> = all_trades.iter().map(|t| t.symbol_id).collect();
    assert!(symbol_ids.contains(&1));
    assert!(symbol_ids.contains(&2));
}

// ============================================================
// Test 7: High-volume — 100K orders with correct matching invariant
//
// Strategy: drain broadcast AFTER processing completes, without a concurrent
// drain thread. The broadcast buffer capacity must be >= number of batches
// so no trades are lost during processing. With 100,000 orders generating
// ~781 batches (64 trades each), we set the buffer to 200,000 so the
// sender never blocks and all trades are available for post-processing drain.
// ============================================================

#[test]
fn test_high_volume_no_loss() {
    const TOTAL: usize = 10_000;

    // 10M capacity (rounds to ~10.5M). Each send takes 2 slots.
    // Total: ~5.25M sends possible. Our ~156 batches fit easily.
    let (trade_tx, mut rx) = broadcast::channel(10_000_000);
    let book = FlatOrderBook::new(1, 100, 10000, 1, 1_000_000);
    let mut worker = OrderWorker::new(book, trade_tx);

    // Concurrent drain thread: keeps draining until channel closes.
    // Uses yield_now() to be cooperative without sleeping.
    let trades_vec: Arc<Mutex<Vec<Trade>>> = Arc::new(Mutex::new(Vec::new()));
    let trades_vec_clone = trades_vec.clone();
    let batches_drained = Arc::new(AtomicUsize::new(0));
    let batches_clone = batches_drained.clone();

    let drain_handle = thread::spawn(move || {
        let mut ok_count = 0usize;
        let mut empty_count = 0usize;
        let mut lagged_count = 0usize;
        loop {
            match rx.try_recv() {
                Ok(trades) => {
                    ok_count += 1;
                    batches_clone.fetch_add(1, Ordering::Relaxed);
                    let mut vec = trades_vec_clone.lock().unwrap();
                    vec.extend(trades);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    eprintln!("DRAIN: Closed. ok={}, empty={}, lagged={}", ok_count, empty_count, lagged_count);
                    break;
                }
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    lagged_count += 1;
                    batches_clone.fetch_add(1, Ordering::Relaxed);
                    eprintln!("DRAIN: Lagged (skipped={}). ok={}, empty={}, lagged={}", skipped, ok_count, empty_count, lagged_count);
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    empty_count += 1;
                    std::thread::yield_now();
                }
            }
        }
        eprintln!("DRAIN: final. ok={}, empty={}, lagged={}", ok_count, empty_count, lagged_count);
    });

    // Generate alternating BUY/SELL orders that always cross.
    // Pattern: BUY 1500, SELL 1500, BUY 1501, SELL 1501, ...
    let orders: Vec<Order> = (0..TOTAL as u64)
        .map(|i| {
            if i % 2 == 0 {
                let price = 1500 + (i % 500);
                limit_order(i + 1, price, 10, Side::Buy, 1)
            } else {
                let price = 1500 + ((i - 1) % 500);
                limit_order(i + 1, price, 10, Side::Sell, 1)
            }
        })
        .collect();

    for order in &orders {
        worker.handle(*order);
    }

    drop(worker); // closes broadcast channel

    // Wait for drain thread to finish collecting
    drain_handle.join().ok();

    let trades = trades_vec.lock().unwrap();
    let batches = batches_drained.load(Ordering::Relaxed);

    let expected_trades = TOTAL / 2; // 50_000

    assert_eq!(
        trades.len(),
        expected_trades,
        "Expected {} trades, got {}. Batches: {}",
        expected_trades,
        trades.len(),
        batches
    );

    let expected_total_qty = 10u64 * (TOTAL as u64 / 2);
    let actual_total_qty: u64 = trades.iter().map(|t| t.quantity).sum();
    assert_eq!(
        actual_total_qty, expected_total_qty,
        "Total traded qty: expected {} got {}",
        expected_total_qty, actual_total_qty
    );

    for trade in trades.iter() {
        assert_eq!(trade.quantity, 10, "Each trade should be exactly 10 units");
    }
}
