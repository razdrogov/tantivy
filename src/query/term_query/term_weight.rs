use async_trait::async_trait;

use super::term_scorer::TermScorer;
use crate::core::SegmentReader;
use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::weight::{for_each_docset, for_each_scorer};
use crate::query::{Explanation, Scorer, Weight};
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, Term};

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    similarity_weight: Bm25Weight,
    scoring_enabled: bool,
}

#[async_trait]
impl Weight for TermWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let term_scorer = self.specialized_scorer(reader, boost)?;
        Ok(Box::new(term_scorer))
    }

    #[cfg(feature = "quickwit")]
    async fn scorer_async(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Box<dyn Scorer>> {
        let term_scorer = self.specialized_scorer_async(reader, boost).await?;
        Ok(Box::new(term_scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.specialized_scorer(reader, 1.0)?;
        if scorer.doc() > doc || scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        let mut explanation = scorer.explain();
        explanation.add_context(format!("Term={:?}", self.term,));
        Ok(explanation)
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        if let Some(alive_bitset) = reader.alive_bitset() {
            Ok(self.scorer(reader, 1.0)?.count(alive_bitset))
        } else {
            let field = self.term.field();
            let inv_index = reader.inverted_index(field)?;
            let term_info = inv_index.get_term_info(&self.term)?;
            Ok(term_info.map(|term_info| term_info.doc_freq).unwrap_or(0))
        }
    }

    #[cfg(feature = "quickwit")]
    async fn count_async(&self, reader: &SegmentReader) -> crate::Result<u32> {
        if let Some(alive_bitset) = reader.alive_bitset() {
            Ok(self.scorer_async(reader, 1.0).await?.count(alive_bitset))
        } else {
            let field = self.term.field();
            let inv_index = reader.inverted_index(field)?;
            let term_info = inv_index.get_term_info_async(&self.term).await?;
            Ok(term_info.map(|term_info| term_info.doc_freq).unwrap_or(0))
        }
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut (dyn FnMut(DocId, Score) + Send),
    ) -> crate::Result<()> {
        let mut scorer = self.specialized_scorer(reader, 1.0)?;
        for_each_scorer(&mut scorer, callback);
        Ok(())
    }

    #[cfg(feature = "quickwit")]
    async fn for_each_async(
        &self,
        reader: &SegmentReader,
        callback: &mut (dyn FnMut(DocId, Score) + Send),
    ) -> crate::Result<()> {
        let mut scorer = self.specialized_scorer_async(reader, 1.0).await?;
        for_each_scorer(&mut scorer, callback);
        Ok(())
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId),
    ) -> crate::Result<()> {
        let mut scorer = self.specialized_scorer(reader, 1.0)?;
        for_each_docset(&mut scorer, callback);
        Ok(())
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    #[cfg(feature = "quickwit")]
    async fn for_each_no_score_async(
        &self,
        reader: &SegmentReader,
        callback: &mut (dyn FnMut(DocId) + Send),
    ) -> crate::Result<()> {
        let mut scorer = self.specialized_scorer_async(reader, 1.0).await?;
        for_each_docset(&mut scorer, callback);
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the TopDocs collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let scorer = self.specialized_scorer(reader, 1.0)?;
        crate::query::boolean_query::block_wand_single_scorer(scorer, threshold, callback);
        Ok(())
    }

    #[cfg(feature = "quickwit")]
    async fn for_each_pruning_async(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut (dyn FnMut(DocId, Score) -> Score + Send),
    ) -> crate::Result<()> {
        let scorer = self.specialized_scorer_async(reader, 1.0).await?;
        crate::query::boolean_query::block_wand_single_scorer(scorer, threshold, callback);
        Ok(())
    }
}

impl TermWeight {
    pub fn new(
        term: Term,
        index_record_option: IndexRecordOption,
        similarity_weight: Bm25Weight,
        scoring_enabled: bool,
    ) -> TermWeight {
        TermWeight {
            term,
            index_record_option,
            similarity_weight,
            scoring_enabled,
        }
    }

    pub fn term(&self) -> &Term {
        &self.term
    }

    pub(crate) fn specialized_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<TermScorer> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field)?;
        let fieldnorm_reader_opt = if self.scoring_enabled {
            reader.fieldnorms_readers().get_field(field)?
        } else {
            None
        };
        let fieldnorm_reader =
            fieldnorm_reader_opt.unwrap_or_else(|| FieldNormReader::constant(reader.max_doc(), 1));
        let similarity_weight = self.similarity_weight.boost_by(boost);
        let postings_opt: Option<SegmentPostings> =
            inverted_index.read_postings(&self.term, self.index_record_option)?;
        if let Some(segment_postings) = postings_opt {
            Ok(TermScorer::new(
                segment_postings,
                fieldnorm_reader,
                similarity_weight,
            ))
        } else {
            Ok(TermScorer::new(
                SegmentPostings::empty(),
                fieldnorm_reader,
                similarity_weight,
            ))
        }
    }

    #[cfg(feature = "quickwit")]
    pub(crate) async fn specialized_scorer_async(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<TermScorer> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index_async(field).await?;
        let fieldnorm_reader_opt = if self.scoring_enabled {
            reader.fieldnorms_readers().get_field_async(field).await?
        } else {
            None
        };
        let fieldnorm_reader =
            fieldnorm_reader_opt.unwrap_or_else(|| FieldNormReader::constant(reader.max_doc(), 1));
        let similarity_weight = self.similarity_weight.boost_by(boost);
        let postings_opt: Option<SegmentPostings> = inverted_index
            .read_postings_async(&self.term, self.index_record_option)
            .await?;
        if let Some(segment_postings) = postings_opt {
            Ok(TermScorer::new(
                segment_postings,
                fieldnorm_reader,
                similarity_weight,
            ))
        } else {
            Ok(TermScorer::new(
                SegmentPostings::empty(),
                fieldnorm_reader,
                similarity_weight,
            ))
        }
    }
}
