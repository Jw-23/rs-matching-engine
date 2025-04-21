use std::
    sync::{Arc, atomic::AtomicUsize}
;

use crate::error::EgineError;

pub trait Producer<T> {
    fn push(&self, ele: T);
    fn push_batch<I>(&self, iter: I) -> usize
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator;

    fn push_slice(&self, slice: &[T]) -> usize
    where
        T: Copy;
}


pub trait Consumer<T> {
    fn take(&self) -> Option<T>;
    fn take_batch(&self, out: &mut [T]) -> usize
    where
        T: Copy;
}
/// 一个 缓冲环建立的时候应该同时返回`tx`和`rx`， 克服所有权的限制
pub trait RingBuffer<T> {
    fn new<P, C>() -> (P, C)
    where
        P: Producer<T>,
        C: Consumer<T>;
}

/// 对齐缓存行大小8字节，防止伪共享—— head和tail在同一个缓存行，修改一个导致缓存行失效
#[repr(align(64))]
#[derive(Debug)]
pub struct Align64<T>(pub T);
#[derive(Debug)]
struct RingBufferInner<T> {
    ptr: *mut T,
    cap: usize,
    head: Align64<AtomicUsize>,
    tail: Align64<AtomicUsize>,
}

unsafe impl<T> Send for RingBufferInner<T> where T: Send {}
unsafe impl<T> Sync for RingBufferInner<T> where T: Send {}
impl<T> RingBufferInner<T>
where
    T: Default + Clone,
{
    fn new(cap: usize) -> Self {
        let mut vec = Vec::with_capacity(cap);
        let new_cap = cap.next_power_of_two();
        // 首先预先填充，否则操作系统很懒，你去要的时候才会分配
        vec.resize(new_cap, T::default());
        let ptr: *mut T = Box::into_raw(vec.into_boxed_slice()) as *mut T;
        Self {
            ptr: ptr,
            cap: new_cap,
            head: Align64(AtomicUsize::new(0)),
            tail: Align64(AtomicUsize::new(0)),
        }
    }
    const fn cap(&self) -> usize {
        self.cap
    }
    /// 在生产环境中发生错误需要立即停止程序
    #[inline(always)]
    fn write(&self, ele: T) -> Result<(), EgineError> {
        // 生产者单线程执行，Relax 这不影响
        let h = self.head.0.load(std::sync::atomic::Ordering::Relaxed);

        // 确保tail的值是最新的, 能够看见
        let t = self.tail.0.load(std::sync::atomic::Ordering::Acquire);

        if h.wrapping_sub(t) >= self.cap {
            return Err(EgineError::Full);
        }

        unsafe {
            std::ptr::write(self.ptr.add(h & (self.cap - 1)), ele);
        }
        self.head
            .0
            .store(h.wrapping_add(1), std::sync::atomic::Ordering::Release);

        Ok(())
    }

    /// 批量写入，一次性提交多个元素，显著减少 atomic 屏障操作
    #[inline(always)]
    fn write_batch<I>(&self, iter: I) -> Result<usize, EgineError>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let iterator = iter.into_iter();
        let count = iterator.len();
        if count == 0 {
            return Ok(0);
        }

        let h = self.head.0.load(std::sync::atomic::Ordering::Relaxed);
        let t = self.tail.0.load(std::sync::atomic::Ordering::Acquire);

        // 如果剩余空间不足，拒绝写入（也可以改成写入能够写入的部分，这里采取全拒绝）
        if h.wrapping_add(count).wrapping_sub(t) > self.cap {
            return Err(EgineError::Full);
        }

        unsafe {
            for (i, ele) in iterator.enumerate() {
                std::ptr::write(self.ptr.add((h.wrapping_add(i)) & (self.cap - 1)), ele);
            }
        }

        // 写入完毕后，一次性提交所有的改动
        self.head
            .0
            .store(h.wrapping_add(count), std::sync::atomic::Ordering::Release);

        Ok(count)
    }

    /// 批量切片写入，利用 ptr::copy_nonoverlapping 达到 memcpy 性能
    #[inline(always)]
    fn write_slice(&self, slice: &[T]) -> Result<usize, EgineError>
    where
        T: Copy,
    {
        let count = slice.len();
        if count == 0 {
            return Ok(0);
        }

        let h = self.head.0.load(std::sync::atomic::Ordering::Relaxed);
        let t = self.tail.0.load(std::sync::atomic::Ordering::Acquire);

        if h.wrapping_add(count).wrapping_sub(t) > self.cap {
            return Err(EgineError::Full);
        }

        let mask = self.cap - 1;
        let index = h & mask;
        let space_to_end = self.cap - index;

        unsafe {
            if count <= space_to_end {
                // 不绕回，一次拷贝
                std::ptr::copy_nonoverlapping(slice.as_ptr(), self.ptr.add(index), count);
            } else {
                // 绕回，分两次拷贝
                std::ptr::copy_nonoverlapping(slice.as_ptr(), self.ptr.add(index), space_to_end);
                std::ptr::copy_nonoverlapping(
                    slice.as_ptr().add(space_to_end),
                    self.ptr,
                    count - space_to_end,
                );
            }
        }

        self.head
            .0
            .store(h.wrapping_add(count), std::sync::atomic::Ordering::Release);

        Ok(count)
    }

    #[inline(always)]
    fn take(&self) -> Option<T> {
        let t = self.tail.0.load(std::sync::atomic::Ordering::Relaxed);

        let h = self.head.0.load(std::sync::atomic::Ordering::Acquire);

        if h == t {
            return None;
        }

        unsafe {
            let index = t & (self.cap - 1);
            let ptr = self.ptr.add(index);
            // 先把数据转移出来，再更新
            let res = std::ptr::read(ptr);

            self.tail
                .0
                .store(t.wrapping_add(1), std::sync::atomic::Ordering::Release);

            Some(res)
        }
    }
}
impl<T> Drop for RingBufferInner<T> {
    fn drop(&mut self) {
        // 这段代码经过编译器优化和cpp直接析构无异
        unsafe {
            let slice_ptr = std::ptr::slice_from_raw_parts_mut(self.ptr, self.cap);
            let _ = Box::from_raw(slice_ptr);
        }
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<RingBufferInner<T>>,
    // 生产者缓存的 tail，大幅减少 Acquire 读带来的 CPU 缓存一致性开销
    cached_tail: Align64<std::sync::atomic::AtomicUsize>,
}

impl<T> Producer<T> for Sender<T>
where
    T: Default + Clone,
{
    fn push(&self, ele: T) {
        let h = self.inner.head.0.load(std::sync::atomic::Ordering::Relaxed);
        let mut t = self.cached_tail.0.load(std::sync::atomic::Ordering::Relaxed);

        if h.wrapping_sub(t) >= self.inner.cap {
            // 缓存的 tail 认为满了，此时才去真正地获取一次真实的 tail
            t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            // TODO:
            while h.wrapping_sub(t) >= self.inner.cap {
                std::hint::spin_loop();
                t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
                self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            }
        }

        unsafe {
            std::ptr::write(self.inner.ptr.add(h & (self.inner.cap - 1)), ele);
        }
        self.inner.head.0.store(h.wrapping_add(1), std::sync::atomic::Ordering::Release);
    }

    fn push_batch<I>(&self, iter: I) -> usize
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let iterator = iter.into_iter();
        let count = iterator.len();
        if count == 0 {
            return 0;
        }

        let h = self.inner.head.0.load(std::sync::atomic::Ordering::Relaxed);
        let mut t = self.cached_tail.0.load(std::sync::atomic::Ordering::Relaxed);

        if h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
            t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            while h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
                std::hint::spin_loop();
                t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
                self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            }
        }

        unsafe {
            for (i, ele) in iterator.enumerate() {
                std::ptr::write(self.inner.ptr.add((h.wrapping_add(i)) & (self.inner.cap - 1)), ele);
            }
        }

        self.inner.head.0.store(h.wrapping_add(count), std::sync::atomic::Ordering::Release);
        count
    }

    fn push_slice(&self, slice: &[T]) -> usize
    where
        T: Copy,
    {
        let count = slice.len();
        if count == 0 {
            return 0;
        }

        let h = self.inner.head.0.load(std::sync::atomic::Ordering::Relaxed);
        let mut t = self.cached_tail.0.load(std::sync::atomic::Ordering::Relaxed);

        if h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
            t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            // TODO: 此处需要添加尝试
            while h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
                std::hint::spin_loop();
                t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
                self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let mask = self.inner.cap - 1;
        let index = h & mask;
        let space_to_end = self.inner.cap - index;

        unsafe {
            if count <= space_to_end {
                std::ptr::copy_nonoverlapping(slice.as_ptr(), self.inner.ptr.add(index), count);
            } else {
                std::ptr::copy_nonoverlapping(slice.as_ptr(), self.inner.ptr.add(index), space_to_end);
                std::ptr::copy_nonoverlapping(
                    slice.as_ptr().add(space_to_end),
                    self.inner.ptr,
                    count - space_to_end,
                );
            }
        }

        self.inner.head.0.store(h.wrapping_add(count), std::sync::atomic::Ordering::Release);
        count
    }
}
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<RingBufferInner<T>>,
    // 消费者缓存的 head，避免频繁的高成本 Acquire 读最新 head
    cached_head: Align64<std::sync::atomic::AtomicUsize>,
}

impl<T> Consumer<T> for Receiver<T>
where
    T: Default + Clone,
{
    fn take(&self) -> Option<T> {
        let t = self.inner.tail.0.load(std::sync::atomic::Ordering::Relaxed);
        let mut h = self.cached_head.0.load(std::sync::atomic::Ordering::Relaxed);

        if h == t {
            // 缓存的 head 认为队列是空的，再去获取一次真实的 head
            h = self.inner.head.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_head.0.store(h, std::sync::atomic::Ordering::Relaxed);
            if h == t {
                return None;
            }
        }

        unsafe {
            let index = t & (self.inner.cap - 1);
            let ptr = self.inner.ptr.add(index);
            let res = std::ptr::read(ptr);

            self.inner.tail.0.store(t.wrapping_add(1), std::sync::atomic::Ordering::Release);

            Some(res)
        }
    }

    fn take_batch(&self, out: &mut [T]) -> usize 
    where
        T: Copy
    {
        let count = out.len();
        if count == 0 {
            return 0;
        }

        let t = self.inner.tail.0.load(std::sync::atomic::Ordering::Relaxed);
        let mut h = self.cached_head.0.load(std::sync::atomic::Ordering::Relaxed);

        let mut available = h.wrapping_sub(t);
        if available == 0 {
            h = self.inner.head.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_head.0.store(h, std::sync::atomic::Ordering::Relaxed);
            available = h.wrapping_sub(t);
            if available == 0 {
                return 0;
            }
        }

        let to_take = std::cmp::min(count, available);
        let mask = self.inner.cap - 1;
        let index = t & mask;
        let space_to_end = self.inner.cap - index;

        unsafe {
            if to_take <= space_to_end {
                std::ptr::copy_nonoverlapping(self.inner.ptr.add(index), out.as_mut_ptr(), to_take);
            } else {
                std::ptr::copy_nonoverlapping(self.inner.ptr.add(index), out.as_mut_ptr(), space_to_end);
                std::ptr::copy_nonoverlapping(
                    self.inner.ptr,
                    out.as_mut_ptr().add(space_to_end),
                    to_take - space_to_end,
                );
            }
        }

        self.inner.tail.0.store(t.wrapping_add(to_take), std::sync::atomic::Ordering::Release);
        to_take
    }
}
/// 必须实现default， 在这启动的时候想操作系统索要内存，否则操作系统会给空头支票，惰性分配
/// 这是单生产者单消费通道确保性能最高
pub fn channel<T>(cap: usize) -> (Sender<T>, Receiver<T>)
where
    T: Clone + Default,
{
    let buf = Arc::new(RingBufferInner::new(cap));
    (
        Sender { 
            inner: buf.clone(),
            cached_tail: Align64(std::sync::atomic::AtomicUsize::new(0)),
        }, 
        Receiver { 
            inner: buf,
            cached_head: Align64(std::sync::atomic::AtomicUsize::new(0)),
        }
    )
}

#[cfg(test)]
mod test_channel {

    use std::{
        thread::sleep,
        time::{Duration, Instant},
    };

    use crate::mpmc::{Consumer, Producer, RingBufferInner, channel};
    /// 单线程测试程序逻辑是否正确
    #[test]
    fn test_buffer_inner() {
        let inner = RingBufferInner::new(1000);
        inner.write(3).unwrap();
        inner.write(4).unwrap();
        assert_eq!(inner.cap(), 1 << 10, "容量自动扩充到二进制失败");
        assert_eq!(inner.take(), Some(3), "取元素错误");
        assert_eq!(inner.take(), Some(4), "取元素错误");
        assert_eq!(
            inner.tail.0.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "指针偏移错误"
        );
    }
    #[test]
    fn test_channel() {
        let (tx, rx) = channel(1000);
        std::thread::spawn(move || {
            for i in 1..=1000 {
                tx.push(i);
            }
        });
        std::thread::spawn(move || {
            let t1 = Instant::now();
            for i in 1..=1000 {
                assert_eq!(rx.take(), Some(i), "没有拿到数据")
            }
            println!("时间差： {} ns", t1.elapsed().as_nanos())
        });

        sleep(Duration::from_secs(5));
    }
}
