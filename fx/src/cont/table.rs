//! file: table.rs
//! author: Jacob Xie
//! date: 2023/02/14 17:25:37 Tuesday
//! brief: Table

use arrow2::datatypes::{DataType, Schema};
use inherent::inherent;

use crate::ab::{private, FxSeq, Purport, StaticPurport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxTable
// ================================================================================================

/// A scalable FxSeq container
#[derive(Debug, Clone)]
pub struct FxTable<const W: usize, S: FxSeq> {
    schema: Schema,
    data: [S; W],
}

// ================================================================================================
// impl Purport
// ================================================================================================

impl<const W: usize, S> StaticPurport for FxTable<W, S> where S: FxSeq {}

#[inherent]
impl<const W: usize, S> Purport for FxTable<W, S>
where
    S: FxSeq,
{
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// impl Eclectic & EclecticMut
// ================================================================================================

impl<const W: usize, S> private::InnerEclectic for FxTable<W, S>
where
    S: FxSeq,
{
    type Seq = S;

    fn ref_sequences(&self) -> &[Self::Seq] {
        &self.data
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        for (i, arr) in arrays.into_iter().enumerate() {
            if i > W {
                break;
            }
            self.data[i] = arr;
        }

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        let mut res = Vec::new();
        for s in self.data.into_iter() {
            res.push(s);
        }

        res
    }
}

impl<const W: usize, S> private::InnerEclecticMutSeq for FxTable<W, S>
where
    S: FxSeq,
{
    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        &mut self.data
    }
}

// ================================================================================================
// Table methods
// ================================================================================================

impl<const W: usize, S> FxTable<W, S>
where
    S: FxSeq,
{
    pub fn new(data: [S; W]) -> Self {
        Self {
            schema: Self::gen_schema(&data),
            data,
        }
    }

    pub fn new_with_names<I, T>(data: [S; W], names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        Self {
            schema: Self::gen_schema_with_names(&data, names),
            data,
        }
    }

    pub fn try_empty_with_schema(schema: Schema) -> FxResult<Self> {
        if schema.fields.len() != W {
            return Err(FxError::LengthMismatch(schema.fields.len(), W));
        }

        let mut data = [(); W].map(|_| S::new_empty(DataType::Null));

        for (idx, f) in schema.fields.iter().enumerate() {
            data[idx] = S::new_empty(f.data_type.clone())
        }

        Ok(Self { schema, data })
    }

    pub fn data(&self) -> &[S; W] {
        &self.data
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_table {
    use super::*;
    use crate::ab::{Congruent, Eclectic, FromSlice, FromVec};
    use crate::cont::{ArcArr, ArcVec};

    #[test]
    fn create_new_table_success() {
        let a = ArcArr::from_slice(&[None, Some("x")]);
        let b = ArcArr::from_slice(&[None, Some(2), None]);

        let vaa = [a, b];

        let table = FxTable::new(vaa);

        println!("{table:?}",);
    }

    #[test]
    fn create_new_table_name_less_success() {
        let a = ArcArr::from_vec(vec![None, Some("x")]);
        let b = ArcArr::from_vec(vec![None, Some(2), None]);

        let vaa = [a, b];

        let table = FxTable::new_with_names(vaa, ["1"]);

        println!("{table:?}",);
    }

    #[test]
    fn create_new_table_name_more_success() {
        let a = ArcVec::from_slice(&[None, Some("x")]);
        let b = ArcVec::from_slice(&[None, Some(2), None]);

        let vaa = [a, b];

        let table = FxTable::new_with_names(vaa, ["1", "2", "3"]);

        println!("{table:?}",);
    }

    #[test]
    fn test_vec_of_arc_arr() {
        let a = ArcArr::from_vec(vec![None, Some("x")]);
        let b = ArcArr::from_vec(vec![None, Some(2), None]);

        let vaa = vec![a, b];

        println!("{:?}", vaa.lens());
        println!("{:?}", vaa.data_types());
    }

    #[test]
    fn test_vec_of_arc_vec() {
        let a = ArcVec::from_vec(vec![None, Some(1)]);
        let b = ArcVec::from_vec(vec![None, Some("y"), None]);

        let vam = vec![a, b];

        println!("{:?}", vam.lens());
        println!("{:?}", vam.data_types());
    }

    #[test]
    fn test_table_of_arc_arr() {
        let a = ArcArr::from_vec(vec![None, Some("x")]);
        let b = ArcArr::from_vec(vec![None, Some(2), None]);

        let vaa = [a, b];

        let table = FxTable::new(vaa);

        println!("{table:?}");
    }

    #[test]
    fn test_table_of_arc_vec() {
        let a = ArcVec::from_vec(vec![None, Some(1)]);
        let b = ArcVec::from_vec(vec![None, Some("y"), None]);

        let vam = [a, b];

        let table = FxTable::new_with_names(vam, ["a"]);

        println!("{table:?}");
    }

    #[test]
    fn test_table_to_chunk() {
        let a = ArcVec::from_vec(vec![Some(1)]);
        let b = ArcVec::from_vec(vec![None, Some("y"), None]);

        let vam = [a, b];

        let table = FxTable::new_with_names(vam, ["a"]);

        let c = table.take_len_to_chunk(2);
        assert!(c.is_ok());

        println!("{:?}", c.unwrap());
    }
}
