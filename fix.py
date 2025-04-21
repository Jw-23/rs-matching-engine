with open("src/mpmc.rs", "r") as f:
    target = f.read()

import re

target = target.replace("""        while h.wrapping_add(count).wrapping_sub(t) > self.inner.cap { std::hint::spin_loop(); t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire); self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed); }
            t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            while h.wrapping_add(count).wrapping_sub(t) > self.inner.cap { std::hint::spin_loop(); t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire); self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed); }
                panic!("buffer is full");
            }
        }""", """        if h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
            t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
            self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            while h.wrapping_add(count).wrapping_sub(t) > self.inner.cap {
                std::hint::spin_loop();
                t = self.inner.tail.0.load(std::sync::atomic::Ordering::Acquire);
                self.cached_tail.0.store(t, std::sync::atomic::Ordering::Relaxed);
            }
        }""")

with open("src/mpmc.rs", "w") as f:
    f.write(target)
