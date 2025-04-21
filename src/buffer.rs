use arrayvec::ArrayVec;

use crate::mpmc::Producer;


pub struct Buffer<T>(pub ArrayVec<T, 256>);

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Self(ArrayVec::new())
    }
    pub fn flush<P>(&mut self, p: P)
    where
        P: Producer<T>,
        T: Copy,
    {
        if !self.0.is_empty() {
            p.push_slice(self.0.as_slice());
            self.0.clear();
        }
    }
    /// 返回 true 则成功，false则失败
    pub fn push(&mut self, ele: T) -> bool {
        if !self.0.is_full() {
            self.0.push(ele);
            true
        } else {
            false
        }
    }

    pub fn capacity(&self)->usize{
        self.0.capacity()
    }
}