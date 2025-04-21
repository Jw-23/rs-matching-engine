
pub struct PriceBitset {
    data: Box<[u64]>,
    size: usize,
}

impl PriceBitset {
    pub fn new(size: usize) -> Self {
        let n = (size + 63) / 64;
        Self {
            // 使用 Boxed slice 保证内存连续且大小固定，避免 Vec 的动态扩容开销
            data: vec![0u64; n].into_boxed_slice(),
            size,
        }
    }

    #[inline(always)]
    pub fn set(&mut self, index: usize) {
        if index < self.size {
            // index / 64 等价于 index >> 6
            // index % 64 等价于 index & 63
            self.data[index >> 6] |= 1 << (index & 63);
        }
    }

    #[inline(always)]
    pub fn clear(&mut self, index: usize) {
        if index < self.size {
            self.data[index >> 6] &= !(1 << (index & 63));
        }
    }

    #[inline(always)]
    pub fn clear_all(&mut self) {
        self.data.fill(0);
    }

    #[inline(always)]
    pub fn test(&self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }
        (self.data[index >> 6] & (1 << (index & 63))) != 0
    }

    /// 向上寻找第一个活跃位 (CTZ 指令)
    #[inline(always)]
    pub fn find_first_set(&self, start: usize) -> usize {
        if start >= self.size {
            return self.size;
        }

        let mut idx = start >> 6;
        let bit = start & 63;

        // 构造掩码：保留 bit 及其高位 (相当于 C++ 的 ~0ULL << bit)
        let mask = !0u64 << bit;
        let word = self.data[idx] & mask;

        if word != 0 {
            return (idx << 6) + word.trailing_zeros() as usize;
        }

        idx += 1;
        while idx < self.data.len() {
            if self.data[idx] != 0 {
                return (idx << 6) + self.data[idx].trailing_zeros() as usize;
            }
            idx += 1;
        }
        self.size
    }

    /// 向下寻找第一个活跃位 (CLZ 指令)
    #[inline(always)]
    pub fn find_first_set_down(&self, start: usize) -> usize {
        if self.size == 0 {
            return 0;
        }
        let mut start = start;
        if start >= self.size {
            start = self.size - 1;
        }

        let mut idx = start >> 6;
        let bit = start & 63;

        // 构造掩码：保留 bit 及其低位
        // 如果 bit 是 63，直接全 1；否则使用位移构造
        let mask = if bit == 63 {
            !0u64
        } else {
            (1 << (bit + 1)) - 1
        };
        let word = self.data[idx] & mask;

        if word != 0 {
        
            return (idx << 6) + (63 - word.leading_zeros() as usize);
        }

        while idx > 0 {
            idx -= 1;
            if self.data[idx] != 0 {
                return (idx << 6) + (63 - self.data[idx].leading_zeros() as usize);
            }
        }
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::PriceBitset;

    fn bitset(size: usize) -> PriceBitset {
        PriceBitset::new(size)
    }

    #[test]
    fn test_new_all_clear() {
        let b = bitset(256);
        for i in 0..256 {
            assert!(!b.test(i));
        }
    }

    #[test]
    fn test_set_and_test() {
        let mut b = bitset(200);
        b.set(0);
        b.set(63);
        b.set(64);
        b.set(199);
        assert!(b.test(0));
        assert!(b.test(63));
        assert!(b.test(64));
        assert!(b.test(199));
    }

    #[test]
    fn test_clear() {
        let mut b = bitset(100);
        b.set(10);
        b.set(50);
        b.set(99);
        assert!(b.test(10));
        assert!(b.test(50));
        assert!(b.test(99));

        b.clear(50);
        assert!(b.test(10));
        assert!(!b.test(50));
        assert!(b.test(99));

        b.clear_all();
        assert!(!b.test(10));
        assert!(!b.test(50));
        assert!(!b.test(99));
    }

    #[test]
    fn test_out_of_bounds_noop() {
        let mut b = bitset(64);
        b.set(64);
        b.clear(64);
        assert!(!b.test(64));
    }

    #[test]
    fn test_find_first_set_empty() {
        let b = bitset(128);
        assert_eq!(b.find_first_set(0), 128);
        assert_eq!(b.find_first_set(64), 128);
        assert_eq!(b.find_first_set(127), 128);
    }

    #[test]
    fn test_find_first_set_single() {
        let mut b = bitset(200);
        b.set(50);
        assert_eq!(b.find_first_set(0), 50);
        assert_eq!(b.find_first_set(49), 50);
        assert_eq!(b.find_first_set(50), 50);
        assert_eq!(b.find_first_set(51), 200);
    }

    #[test]
    fn test_find_first_set_word_boundary() {
        let mut b = bitset(200);
        b.set(63);
        b.set(64);
        b.set(127);
        assert_eq!(b.find_first_set(0), 63);
        assert_eq!(b.find_first_set(63), 63);
        assert_eq!(b.find_first_set(64), 64);
        assert_eq!(b.find_first_set(127), 127);
        assert_eq!(b.find_first_set(128), 200);
    }

    #[test]
    fn test_find_first_set_start_in_nonempty_word() {
        let mut b = bitset(200);
        b.set(10);
        b.set(70);
        assert_eq!(b.find_first_set(5), 10);
        assert_eq!(b.find_first_set(10), 10);
        assert_eq!(b.find_first_set(11), 70);
    }

    #[test]
    fn test_find_first_set_all_set() {
        let mut b = bitset(64);
        for i in 0..64 {
            b.set(i);
        }
        assert_eq!(b.find_first_set(0), 0);
        assert_eq!(b.find_first_set(30), 30);
        assert_eq!(b.find_first_set(63), 63);
    }

    #[test]
    fn test_find_first_set_start_beyond_size() {
        let mut b = bitset(100);
        b.set(50);
        assert_eq!(b.find_first_set(100), 100);
        assert_eq!(b.find_first_set(200), 100);
    }

    #[test]
    fn test_find_first_set_down_empty() {
        let b = bitset(128);
        assert_eq!(b.find_first_set_down(0), 128);
        assert_eq!(b.find_first_set_down(64), 128);
    }

    #[test]
    fn test_find_first_set_down_single() {
        let mut b = bitset(200);
        b.set(50);
        assert_eq!(b.find_first_set_down(200), 50);
        assert_eq!(b.find_first_set_down(50), 50);
        assert_eq!(b.find_first_set_down(49), 200); // 找不到时返回 size
    }

    #[test]
    fn test_find_first_set_down_word_boundary() {
        let mut b = bitset(200);
        b.set(63);
        b.set(64);
        b.set(127);
        assert_eq!(b.find_first_set_down(127), 127);
        assert_eq!(b.find_first_set_down(126), 64);
        assert_eq!(b.find_first_set_down(63), 63);
        assert_eq!(b.find_first_set_down(62), 200); // 找不到时返回 size
    }

    #[test]
    fn test_find_first_set_down_start_in_nonempty_word() {
        let mut b = bitset(200);
        b.set(10);
        b.set(70);
        assert_eq!(b.find_first_set_down(80), 70);
        assert_eq!(b.find_first_set_down(70), 70);
        assert_eq!(b.find_first_set_down(69), 10);
    }

    #[test]
    fn test_find_first_set_down_all_set() {
        let mut b = bitset(64);
        for i in 0..64 {
            b.set(i);
        }
        assert_eq!(b.find_first_set_down(63), 63);
        assert_eq!(b.find_first_set_down(30), 30);
        assert_eq!(b.find_first_set_down(0), 0);
    }

    #[test]
    fn test_find_first_set_down_start_beyond_size() {
        let mut b = bitset(100);
        b.set(50);
        b.set(99);
        assert_eq!(b.find_first_set_down(200), 99);
    }

    #[test]
    fn test_find_first_set_down_empty_size_zero() {
        let b = bitset(0);
        assert_eq!(b.find_first_set_down(0), 0);
        assert_eq!(b.find_first_set_down(100), 0);
    }

    #[test]
    fn test_set_idempotent() {
        let mut b = bitset(64);
        b.set(10);
        b.set(10);
        assert!(b.test(10));
        b.clear(10);
        b.clear(10);
        assert!(!b.test(10));
    }

    #[test]
    fn test_large_bitset() {
        let mut b = bitset(10000);
        b.set(0);
        b.set(63);
        b.set(64);
        b.set(9999);
        assert!(b.test(0));
        assert!(b.test(63));
        assert!(b.test(64));
        assert!(b.test(9999));
        assert!(!b.test(1));
        assert!(!b.test(9998));
        assert_eq!(b.find_first_set(0), 0);
        assert_eq!(b.find_first_set(1), 63);
        assert_eq!(b.find_first_set(64), 64);
        assert_eq!(b.find_first_set(65), 9999);
        assert_eq!(b.find_first_set_down(9999), 9999);
        assert_eq!(b.find_first_set_down(9998), 64);
    }

    #[test]
    fn test_not_aligned_to_word() {
        let mut b = bitset(70);
        b.set(1);
        b.set(65);
        b.set(69);
        assert_eq!(b.find_first_set(0), 1);
        assert_eq!(b.find_first_set(1), 1);
        assert_eq!(b.find_first_set(2), 65);
        assert_eq!(b.find_first_set(65), 65);
        assert_eq!(b.find_first_set(66), 69);
        assert_eq!(b.find_first_set_down(69), 69);
        assert_eq!(b.find_first_set_down(68), 65);
        assert_eq!(b.find_first_set_down(66), 65);
        assert_eq!(b.find_first_set_down(64), 1);
    }
}
