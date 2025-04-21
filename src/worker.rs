use crate::{
    order::{Order, OrderType},
    order_book::{FlatOrderBook, OrderBook, Trade},
    strategy::{MatchingStrategy, StandardMatchingStrategy},
};
use arrayvec::ArrayVec;
use tokio::sync::broadcast;

pub trait Worker {
    type Request;
    type Response;
    fn handle(&mut self, req: Self::Request) -> Self::Response;
}

const TRADE_CAP: usize = 64;

pub struct OrderWorker {
    pub book: FlatOrderBook,
    pub trade_buffer: Vec<Trade>,
    pub trade_tx: broadcast::Sender<Vec<Trade>>,
    pub tmp_trades: ArrayVec<Trade, TRADE_CAP>,
}

impl OrderWorker {
    pub fn new(book: FlatOrderBook, trade_tx: broadcast::Sender<Vec<Trade>>) -> Self {
        Self {
            book,
            trade_buffer: Vec::with_capacity(64),
            trade_tx,
            tmp_trades: ArrayVec::new(),
        }
    }
}

impl Worker for OrderWorker {
    type Request = Order;
    type Response = ();

    fn handle(&mut self, mut req: Self::Request) -> Self::Response {
        self.tmp_trades.clear();

        StandardMatchingStrategy::match_order(&mut self.book, &mut req, &mut self.tmp_trades);
        
        for trade in self.tmp_trades.drain(..) {
            self.trade_buffer.push(trade);
            if self.trade_buffer.len() == 64 {
                let batch = std::mem::replace(&mut self.trade_buffer, Vec::with_capacity(64));
                let _ = self.trade_tx.send(batch);
            }
        }

        if !self.trade_buffer.is_empty() {
            let batch = std::mem::replace(&mut self.trade_buffer, Vec::with_capacity(64));
            let _ = self.trade_tx.send(batch);
        }

        if req.quantity > 0 && matches!(req.ty, OrderType::Limit) {
            let _ = self.book.add_order(req);
        }
    }
}