use crate::common::BinarySerializable;
use crate::common::VInt;
use std::io;
use std::io::Read;
use std::io::Write;

/// `Field` is represented by an unsigned 32-bit integer type
/// The schema holds the mapping between field names and `Field` objects.
#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Field(u32);

impl Field {
    /// Create a new field object for the given FieldId.
    pub const fn from_field_id(field_id: u32) -> Field {
        Field(field_id)
    }

    /// Returns a u32 identifying uniquely a field within a schema.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub const fn field_id(&self) -> u32 {
        self.0
    }
}

impl BinarySerializable for Field {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.0.into()).serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        VInt::deserialize(reader).map(|x| Field(x.0 as u32))
    }
}
