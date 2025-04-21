use crate::{
    order::{Order, OrderType, Price, Side},
    order_book::{FlatOrderBook, OrderBook, PriceLevel, Trade},
};
use arrayvec::ArrayVec;

pub trait MatchingStrategy<const TRADE_CAP: usize> {
    fn match_order<B>(book: &mut B, incoming: &mut Order, trades: &mut ArrayVec<Trade, TRADE_CAP>)
    where
        B: OrderBook<OrderType = Order>;
}

#[derive(Debug)]
pub struct StandardMatchingStrategy;

impl<const TRADE_CAP: usize> MatchingStrategy<TRADE_CAP> for StandardMatchingStrategy {
    #[inline(always)]
    fn match_order<B>(book: &mut B, incoming: &mut Order, trades: &mut ArrayVec<Trade, TRADE_CAP>)
    where
        B: OrderBook<OrderType = Order>,
    {
        let maker_side = incoming.side.opposite();

        let mut current_price = match incoming.side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        };

        // 对应 C++ 的外层 while 循环
        while incoming.quantity > 0 {
            if current_price.0.is_none() {
                break; // 无对手盘，结束撮合
            }

            // 限价单的价格穿透保护
            let is_price_met = match incoming.ty {
                OrderType::Market => true,
                OrderType::Limit => {
                    let maker_p = unsafe { current_price.0.unwrap_unchecked() };
                    let taker_p = unsafe { incoming.price.0.unwrap_unchecked() };
                    match incoming.side {
                        Side::Buy => taker_p >= maker_p,
                        Side::Sell => taker_p <= maker_p,
                    }
                }
            };

            if !is_price_met {
                break;
            }

            let mut opt_idx = book.get_level(current_price, maker_side).unwrap().head_index();

            while let Some(idx) = opt_idx {
                if incoming.quantity == 0 {
                    break;
                }

                // 防爆破保护
                if trades.is_full() {
                    return;
                }

                let next_idx = book.next_order_idx(idx);
                let (level, maker) = book.get_level_and_order_mut(current_price, maker_side, idx).unwrap();

                if !maker.active {
                    if Some(idx) == level.head_index() {
                        if let Some(n) = next_idx {
                            level.set_head_index(n); // 惰性滑动头指针
                        }
                    }
                    opt_idx = next_idx;
                    continue;
                }

                let trade_qty = incoming.quantity.min(maker.quantity);

                maker.quantity -= trade_qty;
                incoming.quantity -= trade_qty;

                // 结构体字面量创建成交记录（碾压 Builder 模式）
                trades.push(Trade {
                    symbol_id: incoming.symbol_id,
                    price: current_price.clone(),
                    quantity: trade_qty,
                    market_order_id: maker.id,
                    taker_order_id: incoming.id,
                });

                // 完美还原 C++ 的挂单耗尽逻辑
                if maker.quantity == 0 {
                    maker.active = false;
                    level.decrement_active();

                    if Some(idx) == level.head_index() {
                        if let Some(n) = next_idx {
                            level.set_head_index(n);
                        }
                    }
                }

                opt_idx = next_idx; // 推进到下一个订单
            }

            let active_count = book.get_level(current_price, maker_side).unwrap().active_count();
            // 对应 C++ 里的: if (level.activeCount == 0) { book.askMask.clear(p); ... break; }
            if active_count == 0 {
                // 1. 通知底层 OrderBook 清除该价格的位图，重置档位数据
                book.remove_level(current_price.clone(), maker_side);

                // 2. 利用位图硬件指令极速获取下一个价格 (替代了 C++ 里的 p++ 遍历)
                current_price = match incoming.side {
                    Side::Buy => book.next_active_ask(current_price),
                    Side::Sell => book.next_active_bid(current_price),
                };
            } else {
                // incoming.quantity 已经被吃光了
                break;
            }
        }
    }
}

// ============================================================
// 单元测试
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    /// 创建限价单
    fn limit_order(id: u64, price: u64, qty: u64, side: Side) -> Order {
        Order {
            id,
            price: Price(Some(std::num::NonZeroU64::new(price).unwrap())),
            client_order_id: 0,
            symbol_id: 1,
            quantity: qty,
            side,
            ty: OrderType::Limit,
            active: true,
        }
    }

    /// 创建市价单
    fn market_order(id: u64, qty: u64, side: Side) -> Order {
        Order {
            id,
            price: Price(None),
            client_order_id: 0,
            symbol_id: 1,
            quantity: qty,
            side,
            ty: OrderType::Market,
            active: true,
        }
    }

    /// 创建订单簿，价格范围 100~200，tick=5，共 21 个档位
    fn new_book() -> FlatOrderBook {
        FlatOrderBook::new(1, 100, 200, 5, 64)
    }

    // --- 市价买单测试 ---

    /// 市价买单完全吃掉一笔卖单
    #[test]
    fn test_market_buy_fully_fills_single_maker() {
        let mut book = new_book();

        // 卖方先挂一单：价格 105，数量 100
        book.add_order(limit_order(10, 105, 100, Side::Sell)).unwrap();

        // 市价买方：数量 100
        let mut incoming = market_order(100, 100, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0, "市价买单应全部成交");
        assert_eq!(trades.len(), 1, "应产生 1 笔成交");
        assert_eq!(trades[0].quantity, 100);
        assert_eq!(trades[0].price.0.unwrap().get(), 105);
        assert_eq!(trades[0].market_order_id, 10); // maker
        assert_eq!(trades[0].taker_order_id, 100); // taker
    }

    /// 市价买单数量大于maker，部分成交
    #[test]
    fn test_market_buy_partially_fills_maker() {
        let mut book = new_book();

        // 卖方挂一单：价格 105，数量 30
        book.add_order(limit_order(10, 105, 30, Side::Sell)).unwrap();

        // 市价买方：数量 100（大于 maker 的 30）
        let mut incoming = market_order(100, 100, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 70, "市价买单剩余 70 未成交");
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity, 30);
        assert_eq!(trades[0].price.0.unwrap().get(), 105);
    }

    /// 市价买单穿越多个卖价档位
    #[test]
    fn test_market_buy_eats_multiple_levels() {
        let mut book = new_book();

        // 卖方挂多个档位：价格 105(30) / 110(50) / 115(20)
        book.add_order(limit_order(1, 105, 30, Side::Sell)).unwrap();
        book.add_order(limit_order(2, 110, 50, Side::Sell)).unwrap();
        book.add_order(limit_order(3, 115, 20, Side::Sell)).unwrap();

        // 市价买方：数量 80（吃 105 的 30 + 110 的 50）
        let mut incoming = market_order(100, 80, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0);
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].quantity, 30);
        assert_eq!(trades[0].price.0.unwrap().get(), 105);
        assert_eq!(trades[0].market_order_id, 1);

        assert_eq!(trades[1].quantity, 50);
        assert_eq!(trades[1].price.0.unwrap().get(), 110);
        assert_eq!(trades[1].market_order_id, 2);

        // 最后一档 115 应未被动
        assert!(book.best_ask().0.is_some());
        assert_eq!(book.best_ask().0.unwrap().get(), 115);
    }

    // --- 市价卖单测试 ---

    /// 市价卖单完全吃掉一笔买单
    #[test]
    fn test_market_sell_fully_fills_single_maker() {
        let mut book = new_book();

        // 买方先挂一单：价格 150，数量 100
        book.add_order(limit_order(10, 150, 100, Side::Buy)).unwrap();

        // 市价卖方：数量 100
        let mut incoming = market_order(100, 100, Side::Sell);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity, 100);
        assert_eq!(trades[0].price.0.unwrap().get(), 150);
        assert_eq!(trades[0].market_order_id, 10);
        assert_eq!(trades[0].taker_order_id, 100);
    }

    /// 市价卖单穿越多个买价档位
    #[test]
    fn test_market_sell_eats_multiple_levels() {
        let mut book = new_book();

        // 买方挂多个档位：价格 150(30) / 145(50) / 140(20)
        book.add_order(limit_order(1, 150, 30, Side::Buy)).unwrap();
        book.add_order(limit_order(2, 145, 50, Side::Buy)).unwrap();
        book.add_order(limit_order(3, 140, 20, Side::Buy)).unwrap();

        // 市价卖方：数量 60（吃 150 的 30 + 145 的 30，剩 20 待撮合）
        let mut incoming = market_order(100, 60, Side::Sell);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0);
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].quantity, 30);
        assert_eq!(trades[0].price.0.unwrap().get(), 150);
        assert_eq!(trades[0].market_order_id, 1);

        assert_eq!(trades[1].quantity, 30); // 只吃 145 档的 50 中的 30
        assert_eq!(trades[1].price.0.unwrap().get(), 145);
        assert_eq!(trades[1].market_order_id, 2);
    }

    // --- 限价单测试 ---

    /// 限价买单价格穿透，卖方有单在更低价
    #[test]
    fn test_limit_buy_matches_when_price_satisfied() {
        let mut book = new_book();

        // 卖方挂单：价格 105，数量 50
        book.add_order(limit_order(10, 105, 50, Side::Sell)).unwrap();

        // 限价买方：价格 110，数量 30
        let mut incoming = limit_order(100, 110, 30, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity, 30);
        assert_eq!(trades[0].price.0.unwrap().get(), 105); // 以 maker 价格成交
    }

    /// 限价买单价格不满足（低于 best ask），不成交
    #[test]
    fn test_limit_buy_price_not_satisfied() {
        let mut book = new_book();

        // 卖方挂单：价格 105，数量 50
        book.add_order(limit_order(10, 105, 50, Side::Sell)).unwrap();

        // 限价买方：价格 100（低于 best ask 105），数量 30
        let mut incoming = limit_order(100, 100, 30, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 30, "限价买单不应成交");
        assert!(trades.is_empty(), "不应产生成交记录");
    }

    /// 限价卖单价格穿透，买方有单在更高价
    #[test]
    fn test_limit_sell_matches_when_price_satisfied() {
        let mut book = new_book();

        // 买方挂单：价格 150，数量 50
        book.add_order(limit_order(10, 150, 50, Side::Buy)).unwrap();

        // 限价卖方：价格 145，数量 30
        let mut incoming = limit_order(100, 145, 30, Side::Sell);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 0);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity, 30);
        assert_eq!(trades[0].price.0.unwrap().get(), 150); // 以 maker 价格成交
    }

    /// 限价卖单价格不满足（高于 best bid），不成交
    #[test]
    fn test_limit_sell_price_not_satisfied() {
        let mut book = new_book();

        // 买方挂单：价格 150，数量 50
        book.add_order(limit_order(10, 150, 50, Side::Buy)).unwrap();

        // 限价卖方：价格 155（高于 best bid 150），数量 30
        let mut incoming = limit_order(100, 155, 30, Side::Sell);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 30, "限价卖单不应成交");
        assert!(trades.is_empty());
    }

    // --- 无对手盘测试 ---

    /// 空订单簿上放市价单，不应崩溃，返回空成交
    #[test]
    fn test_market_order_on_empty_book() {
        let mut book = new_book();

        let mut incoming = market_order(100, 50, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 50);
        assert!(trades.is_empty());
    }

    /// 限价单挂到空订单簿，不应崩溃
    #[test]
    fn test_limit_order_on_empty_book() {
        let mut book = new_book();

        let mut incoming = limit_order(100, 150, 50, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(incoming.quantity, 50);
        assert!(trades.is_empty());
    }

    // --- 撮合后订单簿状态测试 ---

    /// 完全成交后，卖盘已空，最优卖价更新为 None
    #[test]
    fn test_best_ask_updated_after_full_fill() {
        let mut book = new_book();

        book.add_order(limit_order(10, 120, 50, Side::Sell)).unwrap();
        assert_eq!(book.best_ask().0.unwrap().get(), 120);

        let mut incoming = market_order(100, 50, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert!(book.best_ask().0.is_none(), "完全成交后 best_ask 应为空");
    }

    /// 多笔 maker 同档位撮合，验证 FIFO 顺序
    #[test]
    fn test_multiple_makers_same_level_fifo() {
        let mut book = new_book();

        // 同价位 105 的三笔卖单
        book.add_order(limit_order(1, 105, 20, Side::Sell)).unwrap();
        book.add_order(limit_order(2, 105, 30, Side::Sell)).unwrap();
        book.add_order(limit_order(3, 105, 10, Side::Sell)).unwrap();

        // 市价买方吃 40（先吃 id=1 的 20，再吃 id=2 的 20）
        let mut incoming = market_order(100, 40, Side::Buy);
        let mut trades: ArrayVec<Trade, 16> = ArrayVec::new();

        StandardMatchingStrategy::match_order(&mut book, &mut incoming, &mut trades);

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].market_order_id, 1);
        assert_eq!(trades[0].quantity, 20);
        assert_eq!(trades[1].market_order_id, 2);
        assert_eq!(trades[1].quantity, 20); // id=2 还剩 10
        assert_eq!(incoming.quantity, 0);
    }
}

