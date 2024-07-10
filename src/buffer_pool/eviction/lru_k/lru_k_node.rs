use std::collections::VecDeque;
use crate::buffer_pool::common::FrameId;
use super::common::Timestamp;

#[derive(Debug)]
pub struct LRUKNode {
    max_accesses: usize,
    history: VecDeque<Timestamp>,
    _frame_id: FrameId,
    is_evictable: bool,
}

impl LRUKNode {
    pub fn new(max_accesses: usize, frame_id: FrameId) -> Self {
        LRUKNode {
            max_accesses: max_accesses,
            history: VecDeque::new(),
            _frame_id: frame_id,
            is_evictable: false,
        }
    }

    pub fn history_length(&self) -> usize {
        self.history.len()
    }

    pub fn front_of_history(&self) -> Option<&Timestamp> {
        self.history.front()
    }

    pub fn record_history(&mut self, timestamp: Timestamp) {
        self.history.push_back(timestamp);
        if self.history.len() > self.max_accesses {
            self.history.pop_front();
        }
    }

    pub fn is_evictable(&self) -> bool {
        self.is_evictable
    }

    pub fn set_evictable(&mut self, is_evictable: bool) {
        self.is_evictable = is_evictable;
    }
}