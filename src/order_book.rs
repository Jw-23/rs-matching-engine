use foldhash::{HashMap, HashMapExt};

use crate::{
    bitmap::PriceBitset,
    order::{Order, OrderId, Price, Side},
};

#[derive(Debug, Default, Clone, Copy)]
struct OrderLocaction {
    // 在pool 中的位置
    idx: usize,
    /// 在价格中的档位
    price: u64,
}

trait OrderBookBase {
    fn add_order(order: Order);
    fn cancel(order_id: OrderId);
}

/// 成交记录结构
#[derive(Debug,Clone, Copy)]
pub struct Trade {
    pub symbol_id: u64,
    pub price: Price,
    pub quantity: u64,
    pub market_order_id: OrderId,
    pub taker_order_id: OrderId,
}

use std::{fmt::Debug, num::NonZeroU32};

/// 定义订单簿操作产生的错误
#[derive(Debug)]
pub enum EngineError {
    InvalidPrice,
    OrderBookFull,
    OrderNotFound,
}

/// 价格档位的操作约束
pub trait PriceLevel {
    /// 绑定的订单类型
    type Order;

    /// 获取遍历的起点游标
    fn head_index(&self) -> Option<usize>;

    /// 更新遍历的起点游标（跳过已死订单）
    fn set_head_index(&mut self, idx: usize);

    /// 获取该档位的订单总长度
    fn len(&self) -> usize;

    /// 扣减活跃订单数量
    fn decrement_active(&mut self);

    /// 获取指定索引的订单的可变引用
    // fn get_order_mut(&mut self, idx: usize) -> Option<&mut Self::Order>;
    fn active_count(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct OrderNode {
    pub order: Order,
    pub prev: Option<NonZeroU32>,
    pub next: Option<NonZeroU32>,
}

pub struct OrderPool {
    nodes: Vec<OrderNode>,
    free_head: Option<NonZeroU32>,
}

impl OrderPool {
    pub fn new(cap: usize) -> Self {
        let new_cap = cap.next_power_of_two();
        let mut nodes = Vec::with_capacity(new_cap);

        for i in 0..new_cap {
            nodes.push(OrderNode {
                order: Order::default(),
                prev: None,
                // 这里的索引必须是 i + 2，因为我们要指向下一个节点
                // 且 NonZeroU32 的 1 对应数组下标 0
                next: if i == new_cap - 1 {
                    None
                } else {
                    NonZeroU32::new((i + 2) as u32)
                },
            });
        }

        Self {
            nodes,
            // 初始空闲头指向第一个节点（数组下标 0 -> 1）
            free_head: NonZeroU32::new(1),
        }
    }

    /// 分配一个节点，返回其在数组中的真实下标 (usize)
    pub fn alloc(&mut self) -> Option<usize> {
        self.free_head.map(|head_nz| {
            let idx = head_nz.get() as usize - 1; // 转换为 0-based 数组下标
            let node = &mut self.nodes[idx];

            // 更新空闲链表头
            self.free_head = node.next;

            // 关键：拿出来用之前，重置节点关系
            node.prev = None;
            node.next = None;

            idx
        })
    }

    /// 归还一个节点，传入其在数组中的真实下标 (usize)
    pub fn dealloc(&mut self, idx: usize) {
        // 将当前位置标记为“空闲”，并插入空闲链表头部
        // 这里的转换必须是 idx + 1
        let node_nz = NonZeroU32::new((idx + 1) as u32).expect("Index out of range");

        self.nodes[idx].next = self.free_head;
        self.free_head = Some(node_nz);
    }

    /// 安全获取节点的引用，注意需要-1，因为是非零整数
    pub fn get_node(&self, nz_idx: NonZeroU32) -> &OrderNode {
        &self.nodes[nz_idx.get() as usize - 1]
    }

    /// 安全获取节点的可变引用
    pub fn get_node_mut(&mut self, nz_idx: NonZeroU32) -> &mut OrderNode {
        &mut self.nodes[nz_idx.get() as usize - 1]
    }
}

/// 订单簿的核心抽象接口
/// 将存储结构（数组/树/位图）与撮合逻辑彻底解耦
pub trait OrderBook {
    /// 允许实现方指定具体的订单类型
    type OrderType;
    /// 允许实现方指定具体的价格档位类型（例如暴露内部的 ArrayVec）
    type LevelType: PriceLevel<Order = Self::OrderType>;

    // --------------------------------------------------------
    // 核心写操作
    // --------------------------------------------------------

    /// 插入新订单 (无论是 Limit 还是 Market)
    fn add_order(&mut self, order: Self::OrderType) -> Result<(), EngineError>;

    /// 撤销订单
    /// 返回被撤销的订单本体，如果不存在则返回 None
    fn cancel_order(&mut self, order_id: OrderId) -> Option<Self::OrderType>;

    // --------------------------------------------------------
    // 行情与状态读取 (O(1) 极速接口)
    // --------------------------------------------------------

    /// 获取最佳买价。如果买盘为空，返回 Price(None)
    fn best_bid(&self) -> Price;

    /// 获取最佳卖价。如果卖盘为空，返回 Price(None)
    fn best_ask(&self) -> Price;

    /// 获取下一个有活跃订单的买价 (用于市价单或大单吃透深度时向下遍历)
    /// - start_price: 从哪个价格开始往下找
    /// - 返回值: 找到的价格，如果到底了返回 Price(None)
    fn next_active_bid(&self, start_price: Price) -> Price;

    /// 获取下一个有活跃订单的卖价 (向上遍历)
    fn next_active_ask(&self, start_price: Price) -> Price;

    // --------------------------------------------------------
    // 档位微操接口 (供撮合引擎扣减数量使用)
    // --------------------------------------------------------

    /// 获取指定价格档位的可变引用
    /// 注意：如果传入的是 Price(None) [即市价]，应返回 None，因为市价单没有专属的队列档位
    fn get_level_mut(&mut self, price: Price, side: Side) -> Option<&mut Self::LevelType>;

    /// 获取指定价格档位的只读引用
    fn get_level(&self, price: Price, side: Side) -> Option<&Self::LevelType>;
    /// 通知订单簿：该价格档位已空，请清理相关底层状态（如清除位图对应位，重置最优价等）
    fn remove_level(&mut self, price: Price, side: Side);
    /// 获取订单的可变引用
    fn get_order_mut(&mut self, idx: usize) -> Option<&mut Self::OrderType>;

    /// 获取同时引用级别和订单的可变引用，利用生命周期解决分离借用的问题
    fn get_level_and_order_mut(
        &mut self,
        price: Price,
        side: Side,
        idx: usize,
    ) -> Option<(&mut Self::LevelType, &mut Self::OrderType)>;

    /// 获取链表结构中的下一笔订单的内存下标索引
    fn next_order_idx(&self, idx: usize) -> Option<usize>;

    /// 每日收盘清空盘口
    fn clear(&mut self);
}

// --- 辅助结构实现 ---

#[derive(Debug, Default, Clone)]
pub struct FlatLevel {
    pub head: Option<NonZeroU32>,
    pub tail: Option<NonZeroU32>,
    pub active_count: usize,
}

impl PriceLevel for FlatLevel {
    type Order = Order;

    #[inline]
    fn head_index(&self) -> Option<usize> {
        self.head.map(|nz| nz.get() as usize - 1)
    }

    #[inline]
    fn set_head_index(&mut self, idx: usize) {
        self.head = NonZeroU32::new((idx + 1) as u32);
    }

    #[inline]
    fn len(&self) -> usize {
        self.active_count
    }

    #[inline]
    fn decrement_active(&mut self) {
        if self.active_count > 0 {
            self.active_count -= 1;
        }
    }

    #[inline]
    fn active_count(&self) -> usize {
        self.active_count
    }

    // fn get_order_mut(&mut self, _idx: usize) -> Option<&mut Self::Order> {
    //     None // 在本架构中，建议直接通过 Book.pool 访问
    // }
}

// --- 核心 OrderBook 实现 ---

pub struct FlatOrderBook{
    
    symbol_id: u64,
    pool: OrderPool,
    id_to_loc: HashMap<OrderId, OrderLocaction>,

    // 价格平铺数组
    bids: Vec<FlatLevel>,
    asks: Vec<FlatLevel>,

    // 快速查找有单档位的位图
    bids_bitmap: PriceBitset,
    asks_bitmap: PriceBitset,

    // 配置与状态
    min_price: u64,
    max_price: u64,
    tick_size: u64,

    best_bid_idx: Option<usize>,
    best_ask_idx: Option<usize>,
}

impl FlatOrderBook {
    pub fn new(symbol_id: u64, min: u64, max: u64, tick: u64, cap: usize) -> Self {
        let size = ((max - min) / tick) as usize + 1;
        Self {
            symbol_id,
            pool: OrderPool::new(cap),
            id_to_loc: HashMap::with_capacity(cap),
            bids: vec![FlatLevel::default(); size],
            asks: vec![FlatLevel::default(); size],
            bids_bitmap: PriceBitset::new(size),
            asks_bitmap: PriceBitset::new(size),
            min_price: min,
            max_price: max,
            tick_size: tick,
            best_bid_idx: None,
            best_ask_idx: None,
        }
    }

    #[inline]
    fn price_to_idx(&self, price: u64) -> usize {
        ((price - self.min_price) / self.tick_size) as usize
    }
}

impl OrderBook for FlatOrderBook {
    type OrderType = Order;
    type LevelType = FlatLevel;
    /// 警告： 传入价格为0
    fn add_order(&mut self, order: Self::OrderType) -> Result<(), EngineError> {
        let price_val = match order.price.0 {
            Some(nz) => nz.get(),
            None => return Err(EngineError::InvalidPrice), // 价格为空（如市价单），不能进入挂单队列
        };

        if price_val < self.min_price || price_val > self.max_price {
            return Err(EngineError::InvalidPrice);
        }

        if order.quantity > 0 {
            let p_idx = self.price_to_idx(price_val);
            let order_id = order.id;
            let order_price = order.price;
            let side = order.side;

            let pool_idx = self.pool.alloc().ok_or(EngineError::OrderBookFull)?;

            // 写入数据
            self.pool.nodes[pool_idx].order = order;
            let nz_pool_idx = NonZeroU32::new((pool_idx + 1) as u32).unwrap();

            // 挂载到对应的 PriceLevel 尾部
            let level = match side {
                Side::Buy => &mut self.bids[p_idx],
                Side::Sell => &mut self.asks[p_idx],
            };

            if let Some(old_tail_nz) = level.tail {
                let old_tail_idx = old_tail_nz.get() as usize - 1;
                self.pool.nodes[old_tail_idx].next = Some(nz_pool_idx);
                self.pool.nodes[pool_idx].prev = Some(old_tail_nz);
                level.tail = Some(nz_pool_idx);
            } else {
                // tail=None：该档位队列为空（是全新的档位）
                level.head = Some(nz_pool_idx);
                level.tail = Some(nz_pool_idx);
                // 更新位图和最优价
                match side {
                    Side::Buy => {
                        self.bids_bitmap.set(p_idx);
                        if self.best_bid_idx.map_or(true, |b| p_idx > b) {
                            self.best_bid_idx = Some(p_idx);
                        }
                    }
                    Side::Sell => {
                        self.asks_bitmap.set(p_idx);
                        if self.best_ask_idx.map_or(true, |a| p_idx < a) {
                            self.best_ask_idx = Some(p_idx);
                        }
                    }
                }
            }
            level.active_count += 1;

            // 注册索引
            self.id_to_loc.insert(
                order_id,
                OrderLocaction {
                    idx: pool_idx,
                    price: order_price.0.unwrap().get(),
                },
            );
        }

        Ok(())
    }

    fn cancel_order(&mut self, order_id: OrderId) -> Option<Self::OrderType> {
        let loc = self.id_to_loc.remove(&order_id)?;
        let p_idx = self.price_to_idx(loc.price);
        let pool_idx = loc.idx;

        let side = self.pool.nodes[pool_idx].order.side;
        let level = match side {
            Side::Buy => &mut self.bids[p_idx],
            Side::Sell => &mut self.asks[p_idx],
        };

        // 双向链表脱钩逻辑
        let prev_nz = self.pool.nodes[pool_idx].prev;
        let next_nz = self.pool.nodes[pool_idx].next;

        if let Some(p) = prev_nz {
            self.pool.nodes[p.get() as usize - 1].next = next_nz;
        } else {
            level.head = next_nz;
        }

        if let Some(n) = next_nz {
            self.pool.nodes[n.get() as usize - 1].prev = prev_nz;
        } else {
            level.tail = prev_nz;
        }

        level.decrement_active();
        let is_empty = level.head.is_none();

        let order = std::mem::take(&mut self.pool.nodes[pool_idx].order);
        self.pool.dealloc(pool_idx);

        if is_empty {
            self.remove_level(loc.price.into(), side);
        }

        Some(order)
    }

    fn best_bid(&self) -> Price {
        self.best_bid_idx
            .map(|i| (self.min_price + i as u64 * self.tick_size).into())
            .unwrap_or(Price(None))
    }

    fn best_ask(&self) -> Price {
        self.best_ask_idx
            .map(|i| (self.min_price + i as u64 * self.tick_size).into())
            .unwrap_or(Price(None))
    }

    fn next_active_bid(&self, start_price: Price) -> Price {
        let start_idx = self.price_to_idx(start_price.0.unwrap().get());
        if start_idx == 0 {
            return Price(None);
        }
        let next_idx = self.bids_bitmap.find_first_set_down(start_idx - 1);
        if next_idx < self.bids.len() {
            (self.min_price + next_idx as u64 * self.tick_size).into()
        } else {
            Price(None)
        }
    }

    fn next_active_ask(&self, start_price: Price) -> Price {
        let start_idx = self.price_to_idx(start_price.0.unwrap().get());
        let next_idx = self.asks_bitmap.find_first_set(start_idx + 1);
        if next_idx < self.asks.len() {
            (self.min_price + next_idx as u64 * self.tick_size).into()
        } else {
            Price(None)
        }
    }

    fn get_level_mut(&mut self, price: Price, side: Side) -> Option<&mut Self::LevelType> {
        let idx = self.price_to_idx(price.0.unwrap().get());
        match side {
            Side::Buy => self.bids.get_mut(idx),
            Side::Sell => self.asks.get_mut(idx),
        }
    }

    fn get_level(&self, price: Price, side: Side) -> Option<&Self::LevelType> {
        let idx = self.price_to_idx(price.0.unwrap().get());
        match side {
            Side::Buy => self.bids.get(idx),
            Side::Sell => self.asks.get(idx),
        }
    }

    fn remove_level(&mut self, price: Price, side: Side) {
        let idx = self.price_to_idx(price.0.unwrap().get());
        match side {
            Side::Buy => {
                self.bids_bitmap.clear(idx);
                if Some(idx) == self.best_bid_idx {
                    if idx == 0 {
                        self.best_bid_idx = None;
                    } else {
                        let next_best = self.bids_bitmap.find_first_set_down(idx - 1);
                        if next_best < self.bids.len() {
                            self.best_bid_idx = Some(next_best);
                        } else {
                            self.best_bid_idx = None;
                        }
                    }
                }
            }
            Side::Sell => {
                self.asks_bitmap.clear(idx);
                if Some(idx) == self.best_ask_idx {
                    let next_best = self.asks_bitmap.find_first_set(idx + 1);
                    if next_best < self.asks.len() {
                        self.best_ask_idx = Some(next_best);
                    } else {
                        self.best_ask_idx = None;
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.id_to_loc.clear();
        self.bids.iter_mut().for_each(|l| *l = FlatLevel::default());
        self.asks.iter_mut().for_each(|l| *l = FlatLevel::default());
        self.bids_bitmap.clear_all();
        self.asks_bitmap.clear_all();
        self.best_bid_idx = None;
        self.best_ask_idx = None;
        // 注意：pool 建议重新 new 或者通过重置指针方式清空
    }

    fn get_order_mut(&mut self, idx: usize) -> Option<&mut Self::OrderType> {
        let index = NonZeroU32::new((idx + 1) as u32).unwrap();
        let order = &mut self.pool.get_node_mut(index).order;
        Some(order)
    }

    fn get_level_and_order_mut(
        &mut self,
        price: Price,
        side: Side,
        idx: usize,
    ) -> Option<(&mut Self::LevelType, &mut Self::OrderType)> {
        let p_idx = self.price_to_idx(price.0.unwrap().get());
        let level = match side {
            Side::Buy => self.bids.get_mut(p_idx)?,
            Side::Sell => self.asks.get_mut(p_idx)?,
        };
        let index = NonZeroU32::new((idx + 1) as u32).unwrap();
        let order = &mut self.pool.get_node_mut(index).order;
        Some((level, order))
    }

    fn next_order_idx(&self, idx: usize) -> Option<usize> {
        let index = NonZeroU32::new((idx + 1) as u32).unwrap();
        self.pool
            .get_node(index)
            .next
            .map(|nz| nz.get() as usize - 1)
    }
}

// ============================================================
// 单元测试
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    // --- OrderPool 逻辑测试 ---

    #[test]
    fn test_pool_alloc_dealloc() {
        let mut pool = OrderPool::new(8);

        // 初始：8个节点
        assert_eq!(pool.nodes.len(), 8);

        // 分配8个节点（第8次后池才满）
        let idxs: Vec<usize> = (0..8).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(idxs.len(), 8);

        // 满容量时分配返回 None
        assert!(pool.alloc().is_none());

        // 归还一个节点后可以再分配
        pool.dealloc(idxs[0]);
        assert!(pool.alloc().is_some());
    }

    #[test]
    fn test_pool_free_list_reuse() {
        let mut pool = OrderPool::new(4);

        // 分配并归还，验证索引被正确复用
        let idx1 = pool.alloc().unwrap();
        let idx2 = pool.alloc().unwrap();

        pool.dealloc(idx1);
        pool.dealloc(idx2);

        // 再次分配应该拿到复用后的索引
        let idx1_reuse = pool.alloc().unwrap();
        let idx2_reuse = pool.alloc().unwrap();

        // 验证：之前归还的节点可以被重新分配
        assert!(idx1_reuse == idx1 || idx1_reuse == idx2);
        assert!(idx2_reuse == idx1 || idx2_reuse == idx2);
    }

    // --- FlatOrderBook 逻辑测试 ---

    fn make_order(id: u64, price: u64, qty: u64, side: Side) -> Order {
        Order {
            id,
            price: Price(Some(std::num::NonZeroU64::new(price).unwrap())),
            client_order_id: 0,
            symbol_id: 1,
            quantity: qty,
            side,
            ty: crate::order::OrderType::Limit,
            active: true,
        }
    }

    fn new_book() -> FlatOrderBook {
        // 价格范围: 100~200, tick=5 → 21个档位
        FlatOrderBook::new(1, 100, 200, 5, 64)
    }

    #[test]
    fn test_add_single_order() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();

        println!("BEST BID IDX: {:?}", book.best_bid_idx);
        assert_eq!(book.best_bid().0.unwrap().get(), 150);
        assert_eq!(book.best_ask().0, None); // 卖盘为空
    }

    #[test]
    fn test_add_multiple_orders_same_price() {
        let mut book = new_book();

        // 多笔同价位买单，按 FIFO 排队
        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 150, 200, Side::Buy)).unwrap();
        book.add_order(make_order(3, 150, 50, Side::Buy)).unwrap();

        println!("BEST BID IDX: {:?}", book.best_bid_idx);
        assert_eq!(book.best_bid().0.unwrap().get(), 150);
    }

    #[test]
    fn test_add_multiple_prices_best_bid_ask() {
        let mut book = new_book();

        // 打印初始池的 free list 链
        eprintln!(
            "pool nodes.len()={}, free_head initial={:?}",
            book.pool.nodes.len(),
            book.pool.free_head.map(|n| n.get())
        );
        for i in 0..book.pool.nodes.len() {
            eprintln!(
                "  nodes[{}].next={:?}",
                i,
                book.pool.nodes[i].next.map(|n| n.get())
            );
        }

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 160, 100, Side::Buy)).unwrap();
        book.add_order(make_order(3, 110, 100, Side::Sell)).unwrap();
        book.add_order(make_order(4, 105, 100, Side::Sell)).unwrap();

        eprintln!(
            "after all adds: asks[0]=(h={:?},t={:?}), asks[1]=(h={:?},t={:?}), best_ask={:?}",
            book.asks[0].head,
            book.asks[0].tail,
            book.asks[1].head,
            book.asks[1].tail,
            book.best_ask()
        );

        assert_eq!(book.best_bid().0.unwrap().get(), 160);
        assert_eq!(book.best_ask().0.unwrap().get(), 105);
    }

    #[test]
    fn test_cancel_order() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 150, 100, Side::Buy)).unwrap();

        // 撤销订单1
        let canceled = book.cancel_order(1).unwrap();
        assert_eq!(canceled.id, 1);
        // 最佳买价不变（还有订单2在同价位）
        println!("BEST BID IDX: {:?}", book.best_bid_idx);
        assert_eq!(book.best_bid().0.unwrap().get(), 150);
    }

    #[test]
    fn test_cancel_last_order_in_level_updates_best() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();

        // 撤销最后一笔买单价，最优买价应更新为 None 或次优
        book.cancel_order(1).unwrap();

        assert_eq!(book.best_bid().0, None);
    }

    #[test]
    fn test_cancel_order_in_middle_of_queue() {
        let mut book = new_book();

        // 同价位追加3笔
        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(3, 150, 100, Side::Buy)).unwrap();

        // 撤销中间那笔（id=2）
        let canceled = book.cancel_order(2).unwrap();
        assert_eq!(canceled.id, 2);

        // 剩下1和3，best_bid 价格不变
        println!("BEST BID IDX: {:?}", book.best_bid_idx);
        assert_eq!(book.best_bid().0.unwrap().get(), 150);
    }

    #[test]
    fn test_cancel_order_updates_best_when_top_of_queue() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 160, 100, Side::Buy)).unwrap(); // top of bid

        // 撤销顶价（160），最优买应降为150
        book.cancel_order(2).unwrap();
        println!("BEST BID IDX: {:?}", book.best_bid_idx);
        assert_eq!(book.best_bid().0.unwrap().get(), 150);

        // 再撤销150，最优买为空
        book.cancel_order(1).unwrap();
        assert_eq!(book.best_bid().0, None);
    }

    #[test]
    fn test_cancel_non_existent_order() {
        let mut book = new_book();
        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();

        // 撤销不存在的订单
        assert!(book.cancel_order(999).is_none());
    }

    #[test]
    fn test_add_invalid_price() {
        let mut book = new_book();

        // 超出价格范围
        let result = book.add_order(make_order(1, 50, 100, Side::Buy));
        assert!(result.is_err());

        let result = book.add_order(make_order(2, 250, 100, Side::Buy));
        assert!(result.is_err());
    }

    #[test]
    fn test_next_active_bid() {
        let mut book = new_book();

        book.add_order(make_order(1, 120, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(3, 180, 100, Side::Buy)).unwrap();

        // 从180往下找第一个有订单的档位
        let start = Price(Some(std::num::NonZeroU64::new(190).unwrap()));
        let next = book.next_active_bid(start);
        assert_eq!(next.0.unwrap().get(), 180);

        // 从160往下找 → 150
        let start = Price(Some(std::num::NonZeroU64::new(160).unwrap()));
        let next = book.next_active_bid(start);
        assert_eq!(next.0.unwrap().get(), 150);

        // 从150往下找 → 120
        let start = Price(Some(std::num::NonZeroU64::new(150).unwrap()));
        let next = book.next_active_bid(start);
        assert_eq!(next.0.unwrap().get(), 120);

        // 从120往下找 → 没有更低的了
        let start = Price(Some(std::num::NonZeroU64::new(120).unwrap()));
        let next = book.next_active_bid(start);
        assert_eq!(next.0, None);
    }

    #[test]
    fn test_next_active_ask() {
        let mut book = new_book();

        book.add_order(make_order(1, 110, 100, Side::Sell)).unwrap();
        book.add_order(make_order(2, 140, 100, Side::Sell)).unwrap();
        book.add_order(make_order(3, 170, 100, Side::Sell)).unwrap();

        // 从100往上找第一个有订单的档位
        let start = Price(Some(std::num::NonZeroU64::new(100).unwrap()));
        let next = book.next_active_ask(start);
        assert_eq!(next.0.unwrap().get(), 110);

        // 从150往上找 → 170
        let start = Price(Some(std::num::NonZeroU64::new(150).unwrap()));
        let next = book.next_active_ask(start);
        assert_eq!(next.0.unwrap().get(), 170);

        // 从180往上找 → 没有更高的了
        let start = Price(Some(std::num::NonZeroU64::new(180).unwrap()));
        let next = book.next_active_ask(start);
        assert_eq!(next.0, None);
    }

    #[test]
    fn test_clear() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 110, 100, Side::Sell)).unwrap();

        book.clear();

        assert_eq!(book.best_bid().0, None);
        assert_eq!(book.best_ask().0, None);
        assert!(book.id_to_loc.is_empty());
    }

    #[test]
    fn test_bid_and_ask_independent() {
        let mut book = new_book();

        book.add_order(make_order(1, 150, 100, Side::Buy)).unwrap();
        book.add_order(make_order(2, 110, 100, Side::Sell)).unwrap();
        eprintln!(
            "after adds 1&2: best_bid={:?}, best_ask={:?}",
            book.best_bid(),
            book.best_ask()
        );

        book.add_order(make_order(3, 160, 100, Side::Buy)).unwrap();
        eprintln!("after add 3: best_bid={:?}", book.best_bid());
        book.cancel_order(2).unwrap();
        eprintln!("after cancel 2: best_ask={:?}", book.best_ask());
        book.add_order(make_order(4, 105, 100, Side::Sell)).unwrap();
        eprintln!("after add 4: best_ask={:?}", book.best_ask());

        assert_eq!(book.best_bid().0.unwrap().get(), 160);
        assert_eq!(book.best_ask().0.unwrap().get(), 105);
    }

    // ============================================================
    // 性能压测：1亿订单数据，1000万池容量，5000万次添加 + 5000万次取消交替
    // ============================================================
    #[test]
    #[ignore] // 需要 `cargo test -- --ignored` 单独运行
    fn test_perf_50m_add_cancel() {
        const POOL_CAP: usize = 10_000_000; // 1000万
        const TOTAL_OPS: usize = 100_000_000; // 1亿次操作（5000万add + 5000万cancel）

        // 价格范围: 10000~20000, tick=1 → 10001个档位
        let mut book = FlatOrderBook::new(1, 10000, 20000, 1, POOL_CAP);

        let start = std::time::Instant::now();

        // 交替添加和撤销
        // 策略：用 ping-pong ID，每次添加后立即撤销上上笔，保持池中约 POOL_CAP 个活跃订单
        // 先预热：填满前 POOL_CAP 个
        let mut next_id: u64 = 1;

        // 预填充
        for i in 0..POOL_CAP {
            let price = 10000 + (i as u64 % 10001);
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            book.add_order(Order {
                id: next_id,
                price: Price(Some(std::num::NonZeroU64::new(price).unwrap())),
                client_order_id: 0,
                symbol_id: 1,
                quantity: 1,
                side,
                ty: crate::order::OrderType::Limit,
                active: true,
            })
            .unwrap();
            next_id += 1;
        }

        let warmup_time = start.elapsed();
        eprintln!("预热 {} 条订单耗时: {:?}", POOL_CAP, warmup_time);

        // 交替 add/cancel：每次操作 = 1次添加 + 1次撤销
        let ops = TOTAL_OPS / 2;
        for i in 0..ops {
            let cancel_id = (i as u64) + 1; // 撤销最早的那笔
            let _ = book.cancel_order(cancel_id);

            let price = 10000 + ((i as u64 + 1000) % 10001);
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            book.add_order(Order {
                id: next_id,
                price: Price(Some(std::num::NonZeroU64::new(price).unwrap())),
                client_order_id: 0,
                symbol_id: 1,
                quantity: 1,
                side,
                ty: crate::order::OrderType::Limit,
                active: true,
            })
            .unwrap();
            next_id += 1;
        }

        let total_time = start.elapsed();
        let nanos = total_time.as_nanos() as f64;
        let ops_f = TOTAL_OPS as f64;
        let ns_per_op = nanos / ops_f;

        eprintln!("========================================");
        eprintln!("性能压测结果：");
        eprintln!("  总操作数: {} (add + cancel)", TOTAL_OPS);
        eprintln!("  池容量: {}", POOL_CAP);
        eprintln!("  预热耗时: {:?}", warmup_time);
        eprintln!("  总耗时: {:?}", total_time);
        eprintln!("  单次操作耗时: {:.1} ns", ns_per_op);
        eprintln!(
            "  吞吐量: {:.2} M ops/sec",
            1_000_000_000.0 / ns_per_op / 1_000_000.0
        );
        eprintln!("========================================");

        // 验证 book 仍然有效（best_bid/ask 正常）
        let _ = book.best_bid();
        let _ = book.best_ask();
    }
}
