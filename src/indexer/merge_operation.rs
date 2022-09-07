use std::collections::HashSet;
use std::ops::Deref;

use crate::{Inventory, Opstamp, SegmentId, TrackedObject};

#[derive(Default)]
pub(crate) struct MergeOperationInventory(Inventory<InnerMergeOperation>);

impl Deref for MergeOperationInventory {
    type Target = Inventory<InnerMergeOperation>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MergeOperationInventory {
    pub fn segment_in_merge(&self) -> HashSet<SegmentId> {
        let mut segment_in_merge = HashSet::default();
        for merge_op in self.list() {
            for &segment_id in &merge_op.segment_ids {
                segment_in_merge.insert(segment_id);
            }
        }
        segment_in_merge
    }
}

/// A `MergeOperation` has two roles.
/// It carries all of the information required to describe a merge:
/// - `target_opstamp` is the opstamp up to which we want to consume the
/// delete queue and reflect their deletes.
/// - `segment_ids` is the list of segment to be merged.
///
/// The second role is to ensure keep track of the fact that these
/// segments are in merge and avoid starting a merge operation that
/// may conflict with this one.
///
/// This works by tracking merge operations. When considering computing
/// merge candidates, we simply list tracked merge operations and remove
/// their segments from possible merge candidates.
pub struct MergeOperation {
    inner: TrackedObject<InnerMergeOperation>,
}

pub(crate) struct InnerMergeOperation {
    target_opstamp: Opstamp,
    segment_ids: Vec<SegmentId>,
    merged_segment_attributes: Option<serde_json::Value>,
}

impl MergeOperation {
    pub(crate) fn new(
        inventory: &MergeOperationInventory,
        target_opstamp: Opstamp,
        segment_ids: Vec<SegmentId>,
        segment_attributes: Option<serde_json::Value>,
    ) -> MergeOperation {
        let inner_merge_operation = InnerMergeOperation {
            target_opstamp,
            segment_ids,
            merged_segment_attributes: segment_attributes,
        };
        MergeOperation {
            inner: inventory.track(inner_merge_operation),
        }
    }

    pub fn target_opstamp(&self) -> Opstamp {
        self.inner.target_opstamp
    }

    pub fn segment_ids(&self) -> &[SegmentId] {
        &self.inner.segment_ids[..]
    }

    pub fn segment_attributes(&self) -> &Option<serde_json::Value> {
        &self.inner.merged_segment_attributes
    }
}
