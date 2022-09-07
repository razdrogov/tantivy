use crate::query::Weight;
use crate::schema::{Document, Term};
use crate::Opstamp;

/// Policy on how to choose deletion target
pub enum DeleteTarget {
    Query(Box<dyn Weight>),
    Term(Term),
}

/// Timestamped Delete operation.
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    pub target: DeleteTarget,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation {
    pub opstamp: Opstamp,
    pub document: Document,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation {
    /// Add operation
    Add(Document),
    /// Delete operation
    Delete(Term),
}
