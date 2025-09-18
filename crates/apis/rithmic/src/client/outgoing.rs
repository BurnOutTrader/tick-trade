use bytes::Bytes;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use concurrent_queue::ConcurrentQueue;
use tokio::sync::Notify;

pub struct Outgoing {
    pub q: Arc<ConcurrentQueue<Bytes>>, // lock-free MPMC
    pub bell: Arc<Notify>,              // doorbell
    pub has_items: AtomicBool,          // edge detector for bell
}

impl Outgoing {
    pub fn new() -> Self {
        Self {
            q: Arc::new(ConcurrentQueue::unbounded()),
            bell: Arc::new(Notify::new()),
            has_items: AtomicBool::new(false),
        }
    }
    #[inline]
    pub fn push(&self, b: Bytes) {
        let was_empty = self.q.is_empty();
        let _ = self.q.push(b);
        if was_empty && !self.has_items.swap(true, Ordering::AcqRel) {
            self.bell.notify_one();
        }
    }
}