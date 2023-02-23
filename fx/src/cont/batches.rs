//! file: batches.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Batches

use arrow2::datatypes::{Field, Schema};
use inherent::inherent;

use crate::ab::{private, EclecticCollection, Purport, StaticPurport};
use crate::cont::ChunkArr;
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxBatches
// ================================================================================================

#[derive(Debug, Clone, Default)]
pub struct FxBatches {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<ChunkArr>,
}

impl StaticPurport for FxBatches {}

#[inherent]
impl Purport for FxBatches {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl private::InnerEclecticCollection<true, usize, ChunkArr> for FxBatches {
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::<ChunkArr>::new(),
        }
    }

    fn ref_schema(&self) -> Option<&Schema> {
        Some(&self.schema)
    }

    fn ref_container(&self) -> Vec<&ChunkArr> {
        self.data.iter().collect()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&ChunkArr> {
        self.data.get(key)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut ChunkArr> {
        self.data.get_mut(key)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: ChunkArr) -> FxResult<()> {
        if key > self.data.len() {
            return Err(FxError::OutBounds);
        }

        self.data.insert(key, data);

        Ok(())
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        if key > self.data.len() {
            return Err(FxError::OutBounds);
        }

        self.data.remove(key);

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: ChunkArr) -> FxResult<()> {
        self.data.push(data);

        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop();

        Ok(())
    }

    fn take_container(self) -> Vec<ChunkArr> {
        self.data
    }
}

impl FxBatches {
    pub fn new(data: Vec<ChunkArr>) -> Self {
        if data.is_empty() {
            return Self::empty();
        }

        let schema = Self::gen_schema(data.first().unwrap());

        Self { schema, data }
    }

    pub fn new_with_names<I, T>(data: Vec<ChunkArr>, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        if data.is_empty() {
            return Self::empty();
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
mod test_batches {
    use super::*;

    use crate::ab::*;
    use crate::cont::ArcArr;

    #[test]
    fn new_fx_batches() {
        let ca = ChunkArr::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        let bdl = FxBatches::new(vec![ca]);

        println!("{bdl:?}");

        let ca = ChunkArr::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        let bdl = FxBatches::new_with_names(vec![ca], ["c1", "c2"]);

        println!("{bdl:?}");
    }
}
