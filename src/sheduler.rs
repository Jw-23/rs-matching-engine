use std::thread::JoinHandle;

use core_affinity::CoreId;

use crate::{
    mpmc::{Consumer, Sender, channel},
    worker::Worker,
};

/// 调度逻辑

// trait ShedulerHash {
//     fn hash(&self) -> usize;
// }

/// Sheduler 将会给每个核心绑定工作任务
/// 从尾部开始截断 [0,end_core_index]， 留出一部分核心做其他事情
pub struct Sheduler<T, W, const END_CORE_INDEX: usize>
where
    W: Worker<Response = (), Request = T> + Copy,
{
    worker_num: usize,
    // _phantom_data: PhantomData<T>,
    free_cores: Vec<CoreId>,
    worker: W,
    start: bool,
}

impl<T, W, const END_CORE_INDEX: usize> Sheduler<T, W, END_CORE_INDEX>
where
    T: Send + Sync + Clone + 'static + Default,
    W: Worker<Response = (), Request = T> + Copy + Send + Sync + 'static,
{
    /// end_core_index 表示用[0,end_core_index) 内部的核心进行运行
    pub fn new(worker: W) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let mut free_core_ids = Vec::with_capacity(core_ids.len() - END_CORE_INDEX);
        for (idx, id) in core_ids.iter().enumerate() {
            if idx >= END_CORE_INDEX {
                free_core_ids.push(*id);
            }
        }
        Self {
            worker_num: END_CORE_INDEX,
            free_cores: free_core_ids,
            worker: worker,
            start: false,
        }
    }

    pub fn start(&self, channel_cap: usize) -> (Vec<Sender<T>>, Vec<JoinHandle<()>>)
        
    {
        // 机器没有执行核心直接报错
        let core_ids = core_affinity::get_core_ids().unwrap();
        // 直接显示内存泄露，把core_ids变成静态生命周期变量
        let core_ids = Box::leak(core_ids.into_boxed_slice());
        let mut senders: Vec<_> = Vec::with_capacity(END_CORE_INDEX);
        let mut handles = Vec::with_capacity(END_CORE_INDEX);
        for (i, core_id) in core_ids.iter().enumerate() {
            if i >= END_CORE_INDEX {
                continue;
            }
            let (tx, rx) = channel(channel_cap);
            senders.push(tx);
            // 关键路径，有问题直接报错
            let mut worker = self.worker;

            let handle = std::thread::spawn(move || {
                core_affinity::set_for_current(*core_id);
                loop {
                    match rx.take() {
                        Some(m) => worker.handle(m),
                        None => {
                            std::hint::spin_loop();
                        }
                    }
                }
            });
            handles.push(handle);
        }
        (senders, handles)
    }
}
