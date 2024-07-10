use crate::{buffer_pool::common::FrameId, types::CrabDbResult};
use responses::*;

pub trait Replacer {
    fn evict(&mut self) -> CrabDbResult<EvictionResponse>;
    fn record_access(&mut self, frame_id: FrameId) -> CrabDbResult<RecordAccessResponse>;
    fn remove(&mut self, frame_id: FrameId) -> CrabDbResult<RemoveResponse>;
    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> CrabDbResult<SetEvictableResponse>;
    fn size(&self) -> CrabDbResult<ReplacerSizeResponse>;
}

pub mod responses {
    use crate::buffer_pool::common::FrameId;

    #[derive(Debug)]
    pub struct RecordAccessResponse {}
    #[derive(Debug)]
    pub struct RemoveResponse {}
    #[derive(Debug)]
    pub struct SetEvictableResponse {}
    #[derive(Debug)]
    pub struct EvictionResponse {
        frame_id: Option<FrameId>,
    }
    impl EvictionResponse {
        pub fn new(frame_id: Option<FrameId>) -> Self {
            EvictionResponse {
                frame_id
            }
        }
        pub fn frame_id(&self) -> Option<FrameId> {
            self.frame_id
        }
    }
    #[derive(Debug)]
    pub struct ReplacerSizeResponse {
        pub(crate) num_evictable_frames: usize,
    }
    impl ReplacerSizeResponse {
        pub fn num_evictable_frames(&self) -> usize {
            self.num_evictable_frames
        }
    }
}
