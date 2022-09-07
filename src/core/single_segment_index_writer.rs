use crate::indexer::operation::AddOperation;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::SegmentWriter;
use crate::{Directory, Document, Index, IndexMeta, Opstamp, Segment};

#[doc(hidden)]
pub struct SingleSegmentIndexWriter {
    segment_writer: SegmentWriter,
    segment: Segment,
    opstamp: Opstamp,
    segment_attributes: Option<serde_json::Value>,
}

impl SingleSegmentIndexWriter {
    pub fn new(index: Index, mem_budget: usize) -> crate::Result<Self> {
        let segment = index.new_segment();
        let segment_writer = SegmentWriter::for_segment(mem_budget, segment.clone())?;
        Ok(Self {
            segment_writer,
            segment,
            opstamp: 0,
            segment_attributes: None,
        })
    }

    pub fn set_segment_attributes(&mut self, segment_attributes: serde_json::Value) {
        self.segment_attributes = Some(segment_attributes)
    }

    pub fn mem_usage(&self) -> usize {
        self.segment_writer.mem_usage()
    }

    pub fn add_document(&mut self, document: Document) -> crate::Result<()> {
        let opstamp = self.opstamp;
        self.opstamp += 1;
        self.segment_writer
            .add_document(AddOperation { opstamp, document })
    }

    pub fn finalize(self) -> crate::Result<Index> {
        let max_doc = self.segment_writer.max_doc();
        self.segment_writer.finalize()?;
        let segment: Segment = self.segment.with_max_doc(max_doc);
        let index = segment.index();
        let index_meta = IndexMeta {
            index_settings: index.settings().clone(),
            segments: vec![segment.meta().clone()],
            schema: index.schema(),
            opstamp: 0,
            payload: None,
            index_attributes: self.segment_attributes,
        };
        save_metas(&index_meta, index.directory())?;
        index.directory().sync_directory()?;
        Ok(segment.index().clone())
    }
}
