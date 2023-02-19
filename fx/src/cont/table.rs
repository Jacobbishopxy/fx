//! file: table.rs
//! author: Jacob Xie
//! date: 2023/02/14 17:25:37 Tuesday
//! brief: Table

use arrow2::datatypes::Schema;
use inherent::inherent;

use crate::{FxError, FxResult};

use crate::cont::ab::{private, FxSeq, Purport};

// ================================================================================================
// FxTable
// ================================================================================================

/// A scalable FxSeq container
#[derive(Debug, Clone)]
pub struct FxTable<S: FxSeq> {
    schema: Schema,
    data: Vec<S>,
}

#[inherent]
impl<S> Purport for FxTable<S>
where
    S: FxSeq,
{
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<S> FxTable<S>
where
    S: FxSeq,
{
    pub fn new(data: Vec<S>) -> Self {
        Self {
            schema: Self::gen_schema(&data),
            data,
        }
    }

    pub fn new_with_names<I, T>(data: Vec<S>, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        Self {
            schema: Self::gen_schema_with_names(&data, names),
            data,
        }
    }

    pub fn data(&self) -> &Vec<S> {
        &self.data
    }
}

// ================================================================================================
// Impl private::InnerSheaf for Vec<FxSeq>
// ================================================================================================

impl<S> private::InnerEclectic for Vec<S>
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
}

// ================================================================================================
// Impl private::InnerSheaf for FxTable
// ================================================================================================

impl<S> private::InnerEclectic for FxTable<S>
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
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_table {
    // use super::*;

    // use crate::{cont::ab::Eclectic, FxArray, FxVector};

    // #[test]
    // fn create_new_table_succes() {
    //     let a = FxArray::from(vec![None, Some("x")]).into_array();
    //     let b = FxArray::from(vec![None, Some(2), None]).into_array();

    //     let vaa = vec![a, b];

    //     let table = FxTable::new(vaa);

    //     println!("{table:?}",);
    // }

    // #[test]
    // fn create_new_table_name_less_succes() {
    //     let a = FxArray::from(vec![None, Some("x")]).into_array();
    //     let b = FxArray::from(vec![None, Some(2), None]).into_array();

    //     let vaa = vec![a, b];

    //     let table = FxTable::new_with_names(vaa, ["1"]);

    //     println!("{table:?}",);
    // }

    // #[test]
    // fn create_new_table_name_more_succes() {
    //     let a = FxArray::from(vec![None, Some("x")]).into_array();
    //     let b = FxArray::from(vec![None, Some(2), None]).into_array();

    //     let vaa = vec![a, b];

    //     let table = FxTable::new_with_names(vaa, ["1", "2", "3"]);

    //     println!("{table:?}",);
    // }

    // #[test]
    // fn test_vec_of_arc_arr() {
    //     let a = FxArray::from(vec![None, Some("x")]).into_array();
    //     let b = FxArray::from(vec![None, Some(2), None]).into_array();

    //     let vaa = vec![a, b];

    //     println!("{:?}", vaa.lens());
    //     println!("{:?}", vaa.data_types());
    // }

    // #[test]
    // fn test_vec_of_arc_vec() {
    //     let a = FxVector::from(vec![None, Some(1)]).into_mutable_array();
    //     let b = FxVector::from(vec![None, Some("y"), None]).into_mutable_array();

    //     let vam = vec![a, b];

    //     println!("{:?}", vam.lens());
    //     println!("{:?}", vam.data_types());
    // }

    // #[test]
    // fn test_table_of_arc_arr() {
    //     let a = FxArray::from(vec![None, Some("x")]).into_array();
    //     let b = FxArray::from(vec![None, Some(2), None]).into_array();

    //     let vaa = vec![a, b];

    //     let table = FxTable::new(vaa);

    //     println!("{table:?}");
    // }

    // #[test]
    // fn test_table_of_arc_vec() {
    //     let a = FxVector::from(vec![None, Some(1)]).into_mutable_array();
    //     let b = FxVector::from(vec![None, Some("y"), None]).into_mutable_array();

    //     let vam = vec![a, b];

    //     let table = FxTable::new_with_names(vam, ["a"]);

    //     println!("{table:?}");
    // }
}
