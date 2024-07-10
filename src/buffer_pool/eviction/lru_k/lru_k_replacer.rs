use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;

use crate::buffer_pool::{common::FrameId, eviction::replacer::Replacer};
use crate::types::{CrabDBError, CrabDbResult};
use crate::buffer_pool::eviction::replacer::responses::*;

use super::{common::Timestamp, lru_k_node::LRUKNode};

pub struct LRUKReplacer {
    max_accesses: usize,
    replacer_size: usize,
    state: RwLock<LRUKReplacerState>,
}

#[derive(Debug)]
pub struct LRUKReplacerState {
    current_size: usize,
    current_timestamp: Timestamp,
    node_store: HashMap<FrameId, LRUKNode>,
}

impl LRUKReplacer {
    pub fn new(replacer_size: usize, max_accesses: usize) -> Self {
        LRUKReplacer {
            replacer_size,
            max_accesses: max_accesses,
            state: RwLock::new(LRUKReplacerState {
                current_size: 0,
                current_timestamp: 1,
                node_store: HashMap::new(),
            })
        }
    }
}

impl Replacer for LRUKReplacer {
   
    fn record_access(&mut self, frame_id: FrameId) -> CrabDbResult<RecordAccessResponse> {
        let mut lruk_state: RwLockWriteGuard<LRUKReplacerState> = self.state.write().unwrap();
        let current_timestamp = lruk_state.current_timestamp;
        let node = lruk_state.node_store.get_mut(&frame_id);
        match node {
            Some(node) => {
                node.record_history(current_timestamp);
            },
            None => {
                if lruk_state.node_store.len() > self.replacer_size {
                    return Err(CrabDBError::new("Frame cannot exceed replacer size".into()))
                }
                let mut node = LRUKNode::new(self.max_accesses, frame_id);
                node.record_history(current_timestamp);
                lruk_state.node_store.insert(frame_id, node);
            }
        }
        lruk_state.current_timestamp += 1;
        Ok(RecordAccessResponse {  })
    }

    fn evict(&mut self) -> CrabDbResult<EvictionResponse> {
        
        let mut evicted_frame: Option<FrameId> = None;
        let mut max_k_distance = 0;
        {
            let mut lruk_state: RwLockWriteGuard<LRUKReplacerState> = self.state.write().unwrap();
            
            let current_timestamp = lruk_state.current_timestamp;

            for (frame_id, node) in lruk_state.node_store.iter_mut() {
                if !node.is_evictable() {
                    continue
                }
                
                let node_history_length = node.history_length();
                if node_history_length == 0 {
                    panic!("How is the node there in the map if it's history length() is 0?, frame_id {}, Node details: {:?}", frame_id, node);
                }
                let node_earliest_timestamp = node.front_of_history().expect(format!("Can never not have a history when the node has been accessed and present {frame_id}").as_str());

                let start_distance = if node_history_length >= self.max_accesses {
                    current_timestamp
                } else {
                    u64::MAX
                };

                let backwards_k_distance = start_distance - node_earliest_timestamp;

                if backwards_k_distance > max_k_distance {
                    evicted_frame = Some(*frame_id);
                    max_k_distance = backwards_k_distance;
                }
            }
        }
        
        if let Some(frame) = evicted_frame {
            match self.remove(frame) {
                Ok(_) => (),
                Err(e) => return Err(CrabDBError::new(format!("Failed to remove evicted frame from replacer {e}").into()))
            }
        } 

        Ok(EvictionResponse::new(evicted_frame))
        
    }

    fn remove(&mut self, frame_id: FrameId) -> CrabDbResult<RemoveResponse> {
        let mut lruk_state: RwLockWriteGuard<LRUKReplacerState> = self.state.write().unwrap();
        let node = lruk_state.node_store.get(&frame_id);
        match node {
            Some(node) => {
                match node.is_evictable() {
                    true => {
                        lruk_state.node_store.remove(&frame_id);
                        lruk_state.current_size -= 1;
                    },
                    false => return Err(CrabDBError::new("Frame is marked as not evictable".into()))
                }
            },
            None => return Err(CrabDBError::new("Frame doesn't exist; invalid remove command".into()))
        }
        
        Ok(RemoveResponse {})
    }

    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> CrabDbResult<SetEvictableResponse> {
        let mut lruk_state: RwLockWriteGuard<LRUKReplacerState> = self.state.write().unwrap();
        let node = lruk_state.node_store.get_mut(&frame_id);
        if node.is_none() {
            return Err(CrabDBError::new("Frame doesn't exist to set_evictable".into()));
        } 
        
        if let Some(node) = node {
            match node.is_evictable()  {
                true => {
                    match set_evictable {
                        true => (),
                        false => {
                            node.set_evictable(false);
                            lruk_state.current_size -= 1;
                        },
                    }
                },
                false => {
                    match set_evictable {
                        true => {
                            node.set_evictable(true);
                            lruk_state.current_size += 1;
                        },
                        false => (),
                    }
                },
            } 
        }
        
        Ok(SetEvictableResponse {  })
    }

    fn size(&self) -> CrabDbResult<ReplacerSizeResponse> {
        let lruk_state: RwLockReadGuard<LRUKReplacerState> = self.state.read().unwrap();
        Ok(ReplacerSizeResponse { num_evictable_frames: lruk_state.current_size })
    }
    
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::eviction::replacer::Replacer as _;
    use super::LRUKReplacer;

    #[test]
    pub fn test_lru_size_empty() {
        let replacer = LRUKReplacer::new(6, 2);
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());
    }

    #[test]
    pub fn test_lru_record_access_set_evictable_basic() {
        let mut replacer: LRUKReplacer = LRUKReplacer::new(7, 2);
        assert!(replacer.record_access(1).is_ok());
        assert!(replacer.record_access(2).is_ok());
        assert!(replacer.record_access(3).is_ok());
        // nothing is evictable yet
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());
        assert!(replacer.set_evictable(1, true).is_ok());
        assert!(replacer.set_evictable(2, true).is_ok());
        assert_eq!(2, replacer.size().unwrap().num_evictable_frames());
        assert!(replacer.set_evictable(2, false).is_ok());
        assert_eq!(1, replacer.size().unwrap().num_evictable_frames());
        assert!(replacer.set_evictable(1, false).is_ok());
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());
    }

    #[test]
    pub fn test_lru_record_remove_basic() {
        let mut replacer: LRUKReplacer = LRUKReplacer::new(7, 2);
        assert!(replacer.record_access(1).is_ok());
        
        let rm = replacer.remove(1);
        match rm {
            Ok(_) => panic!("Test should have thrown an error!"),
            Err(e) => assert_eq!("Frame is marked as not evictable", e.message())
        }

        assert!(replacer.set_evictable(1, true).is_ok());
        assert!(replacer.remove(1).is_ok());
    }

    #[test]
    pub fn test_lru_k_cmu_test_case() {
        let mut replacer: LRUKReplacer = LRUKReplacer::new(7, 2);

        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
        assert!(replacer.record_access(1).is_ok());
        assert!(replacer.record_access(2).is_ok());
        assert!(replacer.record_access(3).is_ok());
        assert!(replacer.record_access(4).is_ok());
        assert!(replacer.record_access(5).is_ok());
        assert!(replacer.record_access(6).is_ok());
        assert!(replacer.set_evictable(1, true).is_ok());
        assert!(replacer.set_evictable(2, true).is_ok());
        assert!(replacer.set_evictable(3, true).is_ok());
        assert!(replacer.set_evictable(4, true).is_ok());
        assert!(replacer.set_evictable(5, true).is_ok());
        assert!(replacer.set_evictable(6, false).is_ok());
        assert_eq!(5, replacer.size().unwrap().num_evictable_frames());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
        // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
        replacer.record_access(1).unwrap();

        // Scenario: Evict three pages from the replacer. Elements with max k-distance should be
        // popped first based on LRU.
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(2), value);
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(3), value);
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(4), value);
        assert_eq!(replacer.size().unwrap().num_evictable_frames(), 2);

        // Scenario: Now replacer has frames [5,1]. Insert new frames 3, 4, and update access
        // history for 5. We should end with [3,1,5,4]
        assert!(replacer.record_access(3).is_ok());
        assert!(replacer.record_access(4).is_ok());
        assert!(replacer.record_access(5).is_ok());
        assert!(replacer.record_access(4).is_ok());
        assert!(replacer.set_evictable(3, true).is_ok());
        assert!(replacer.set_evictable(4, true).is_ok());
        assert_eq!(4, replacer.size().unwrap().num_evictable_frames());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(3), value);
        assert_eq!(3, replacer.size().unwrap().num_evictable_frames());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
        assert!(replacer.set_evictable(6, true).is_ok());
        assert_eq!(4, replacer.size().unwrap().num_evictable_frames());
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(6), value);
        assert_eq!(3, replacer.size().unwrap().num_evictable_frames());

        // Now we have [1,5,4]. Continue looking for victims.
        assert!(replacer.set_evictable(1, false).is_ok());
        assert_eq!(2, replacer.size().unwrap().num_evictable_frames());
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(5), value);
        assert_eq!(1, replacer.size().unwrap().num_evictable_frames());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        assert!(replacer.record_access(1).is_ok());
        assert!(replacer.record_access(1).is_ok());
        assert!(replacer.set_evictable(1, true).is_ok());
        assert_eq!(2, replacer.size().unwrap().num_evictable_frames());
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(4), value);

        assert_eq!(1, replacer.size().unwrap().num_evictable_frames());
        let value = replacer.evict().unwrap().frame_id();
        assert_eq!(Some(1), value);
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());

        // These operations should not modify size
        assert_eq!(None, replacer.evict().unwrap().frame_id());
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());

        eprintln!("{:?}", replacer.remove(1));
        assert_eq!(
            "Frame doesn't exist; invalid remove command",
            replacer.remove(1).unwrap_err().message()
        );
        
        assert_eq!(0, replacer.size().unwrap().num_evictable_frames());
    }
}