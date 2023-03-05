//! file: batches.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Batches

use arrow2::datatypes::{Field, Schema};
use inherent::inherent;

use crate::ab::*;
use crate::error::FxResult;

// ================================================================================================
// FxBatches
// ================================================================================================

#[derive(Debug, Clone, Default)]
pub struct FxBatches<E: Eclectic> {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<E>,
}

// ================================================================================================
// Purport
// ================================================================================================

impl<E: Eclectic> StaticPurport for FxBatches<E> {}

#[inherent]
impl<E: Eclectic> Purport for FxBatches<E> {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// Receptacle's impl
// ================================================================================================

// ChunkArr -> FxBatches
impl<E: Eclectic> private::InnerReceptacle<true, usize, E> for FxBatches<E> {
    fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::<E>::new(),
        }
    }

    fn ref_schema(&self) -> Option<&Schema> {
        Some(&self.schema)
    }

    fn get_chunk(&self, key: usize) -> FxResult<&E> {
        self.data.get_chunk(key)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut E> {
        self.data.get_mut_chunk(key)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: E) -> FxResult<()> {
        self.data.insert_chunk_type_unchecked(key, data)
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        self.data.remove_chunk(key)
    }

    fn push_chunk_type_unchecked(&mut self, data: E) -> FxResult<()> {
        self.data.push_chunk_type_unchecked(data)
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop_chunk()
    }
}

impl<E: Eclectic> FxBatches<E> {
    pub fn new(data: Vec<E>) -> Self {
        if data.is_empty() {
            return Self::new_empty();
        }

        let schema = Self::gen_schema(data.first().unwrap());

        Self { schema, data }
    }

    pub fn new_with_names<I, T>(data: Vec<E>, names: I) -> Self
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
mod test_batches {
    use super::*;

    use crate::cont::{ArcArr, ChunkArr, FxBatch};

    #[test]
    fn new_fx_batches() {
        let ca = ChunkArr::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        // FxBatches<Chunk<Arc<dyn Array>>>
        let b = FxBatches::new(vec![ca]);

        println!("{b:?}");

        let ca = ChunkArr::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        // FxBatches<Chunk<Arc<dyn Array>>>
        let b = FxBatches::new_with_names(vec![ca], ["c1", "c2"]);

        println!("{b:?}");
    }

    #[test]
    fn new_fx_batches2() {
        let ca = FxBatch::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        // FxBatches<FxBatch>
        let b = FxBatches::new(vec![ca]);

        println!("{b:?}");

        let ca = FxBatch::new(vec![
            ArcArr::from_slice(&["a", "c", "z"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[true, false, false]),
        ]);
        // FxBatches<FxBatch>
        let b = FxBatches::new_with_names(vec![ca], ["c1", "c2"]);

        println!("{b:?}");
    }
}
