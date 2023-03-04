//! file: tables.rs
//! author: Jacob Xie
//! date: 2023/02/23 23:02:32 Thursday
//! brief: Tables

use arrow2::datatypes::{Field, Schema};
use inherent::inherent;

use crate::ab::{private, EclecticCollection, FxSeq, Purport, StaticPurport};
use crate::error::FxResult;

// ================================================================================================
// FxBundles
// ================================================================================================

#[derive(Debug, Clone, Default)]
pub struct FxBundles<const W: usize, S: FxSeq> {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<[S; W]>,
}

impl<const W: usize, S: FxSeq> StaticPurport for FxBundles<W, S> {}

#[inherent]
impl<const W: usize, S: FxSeq> Purport for FxBundles<W, S> {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// [S; W] -> Tables
impl<const W: usize, S: FxSeq> private::InnerEclecticCollection<true, usize, [S; W]>
    for FxBundles<W, S>
{
    fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::<[S; W]>::new(),
        }
    }

    fn ref_schema(&self) -> Option<&Schema> {
        Some(&self.schema)
    }

    fn ref_container(&self) -> Vec<&[S; W]> {
        self.data.ref_container()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&[S; W]> {
        self.data.get(key)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut [S; W]> {
        self.data.get_mut(key)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: [S; W]) -> FxResult<()> {
        self.data.insert_chunk_type_unchecked(key, data)
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        self.data.remove_chunk(key)
    }

    fn push_chunk_type_unchecked(&mut self, data: [S; W]) -> FxResult<()> {
        self.data.push_chunk_type_unchecked(data)
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop_chunk()
    }

    fn take_container(self) -> Vec<[S; W]> {
        self.data
    }
}

impl<const W: usize, S: FxSeq> FxBundles<W, S> {
    pub fn new(data: Vec<[S; W]>) -> Self {
        if data.is_empty() {
            return Self::new_empty();
        }

        let schema = Self::gen_schema(data.first().unwrap());

        Self { schema, data }
    }

    pub fn new_with_names<I, T>(data: Vec<[S; W]>, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        if data.is_empty() {
            return Self::new_empty();
        }

        let schema = Self::gen_schema_with_names(data.first().unwrap(), names);

        Self { schema, data }
    }

    pub fn empty_with_schema(schema: Schema) -> Self {
        let data = Vec::new();
        Self { schema, data }
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_tables {
    use super::*;

    use crate::ab::*;
    use crate::cont::ArcArr;

    #[test]
    fn new_fx_tables() {
        let ca = [
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ];
        let t = FxBundles::new(vec![ca]);

        println!("{t:?}");

        let ca = [
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ];
        let t = FxBundles::new_with_names(vec![ca], ["c1", "c2"]);

        println!("{t:?}");
    }
}
