//! file: table.rs
//! author: Jacob Xie
//! date: 2023/02/14 17:25:37 Tuesday
//! brief: Table

use arrow2::datatypes::Schema;

use crate::{FxError, FxResult};

use super::ab::{private, FxSeq};

// ================================================================================================
// FxTable
// ================================================================================================

/// A scalable FxSeq container
pub struct FxTable<S: FxSeq> {
    schema: Schema,
    data: Vec<S>,
}

impl<S> FxTable<S>
where
    S: FxSeq,
{
    pub fn new() -> Self {
        todo!()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn data(&self) -> &Vec<S> {
        &self.data
    }
}

// ================================================================================================
// Impl private::InnerSheaf for Vec<FxSeq>
// ================================================================================================

impl<S> private::InnerSheaf for Vec<S>
where
    S: FxSeq,
{
    type Seq = S;

    fn empty() -> Self
    where
        Self: Sized,
    {
        Vec::<S>::new()
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.as_slice()
    }

    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        self.as_mut_slice()
    }

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        if !self.data_types_match(&arrays) {
            return Err(FxError::SchemaMismatch);
        }

        *self = arrays;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self
    }

    fn take_chunk(self) -> arrow2::chunk::Chunk<crate::types::ArcArr> {
        todo!()
    }
}

// ================================================================================================
// Impl private::InnerSheaf for FxTable
// ================================================================================================

impl<S> private::InnerSheaf for FxTable<S>
where
    S: FxSeq,
{
    type Seq = S;

    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::default(),
            data: vec![],
        }
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        &self.data
    }

    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        &mut self.data
    }

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        if !self.data_types_match(&arrays) {
            return Err(FxError::SchemaMismatch);
        }

        self.data = arrays;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.data
    }

    fn take_chunk(self) -> arrow2::chunk::Chunk<crate::types::ArcArr> {
        todo!()
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_table {
    use crate::{cont::ab::Sheaf, FxArray, FxVector};

    #[test]
    fn test_vec_of_arc_arr() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        println!("{:?}", vaa.lens());
        println!("{:?}", vaa.data_types());
    }

    #[test]
    fn test_vec_of_arc_vec() {
        let a = FxVector::from(vec![None, Some(1)]).into_mutable_array();
        let b = FxVector::from(vec![None, Some("y"), None]).into_mutable_array();

        let vaa = vec![a, b];

        println!("{:?}", vaa.lens());
        println!("{:?}", vaa.data_types());
    }

    #[test]
    fn test_table_of_arc_arr() {
        unimplemented!();
    }

    #[test]
    fn test_table_of_arc_vec() {
        unimplemented!();
    }
}
