use std::{hash::Hasher, thread::JoinHandle};

use crate::{buffer::Buffer, mpmc::Sender, sheduler::Sheduler, worker::Worker};

struct BufferedSheduler<T, W, const END_CORE_INDEX: usize>
where
    W: Worker<Request = T, Response = ()> + Copy,
{
    sheduler: Sheduler<T, W, END_CORE_INDEX>,
    bufs: [Buffer<T>; END_CORE_INDEX],
}
/// 需要订单可分片
pub trait Shardable {
    fn hash(&self) -> usize;
}
impl<T, W, const END_CORE_INDEX: usize> BufferedSheduler<T, W, END_CORE_INDEX>
where
    T: Send + Sync + Clone + Shardable + 'static + Default,
    W: Worker<Response = (), Request = T> + Copy + Send + Sync + 'static,
{
    pub fn start(&self) -> (Vec<Sender<T>>, Vec<JoinHandle<()>>) {
        self.sheduler.start(1024 * 1000)
    }

    pub fn submit(&self,req:T){
        let idx=req.hash()%END_CORE_INDEX;
        // 一定能够找到的
        let buf=self.bufs.get(idx).unwrap();
        
    }
}
