//! file: bundle.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Bundle

use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;

use crate::NullableOptions;
use crate::{private, Chunking, FxError, FxGrid, FxResult};

#[derive(Debug, Clone)]
pub struct FxBundle {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<FxGrid>,
}

impl private::InnerChunkingContainer<usize, FxGrid> for FxBundle {
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::new(),
        }
    }

    fn ref_container(&self) -> Vec<&FxGrid> {
        self.data.iter().collect()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&FxGrid> {
        self.data
            .get(key)
            .ok_or_else(|| FxError::LengthMismatch(key, self.data.len()))
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut FxGrid> {
        let s_len = self.data.len();
        self.data
            .get_mut(key)
            .ok_or_else(|| FxError::LengthMismatch(key, s_len))
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: FxGrid) -> FxResult<()> {
        let s_len = self.data.len();
        if key > s_len {
            return Err(FxError::LengthMismatch(key, s_len));
        }
        self.data.insert(key, data);
        Ok(())
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        let s_len = self.data.len();
        if key > s_len {
            return Err(FxError::LengthMismatch(key, s_len));
        }
        self.data.remove(key);
        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: FxGrid) -> FxResult<()> {
        self.data.push(data);
        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop();
        Ok(())
    }

    fn take_container(self) -> Vec<FxGrid> {
        self.data
    }
}

impl FxBundle {
    pub fn try_new<I, T>(
        fields_name: I,
        data: FxGrid,
        nullable_options: NullableOptions,
    ) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let schema = nullable_options.gen_schema(fields_name, data.data_types())?;

        Ok(Self {
            schema,
            data: vec![data],
        })
    }

    pub fn new_empty<IN, N, IT, D>(
        fields_name: IN,
        data_types: IT,
        nullable_options: NullableOptions,
    ) -> FxResult<Self>
    where
        IN: IntoIterator<Item = N>,
        N: AsRef<str>,
        IT: IntoIterator<Item = D>,
        D: Into<DataType>,
    {
        let schema = nullable_options.gen_schema(fields_name, data_types)?;

        Ok(Self {
            schema,
            data: vec![],
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_batches {
    use super::*;
    use crate::{FromSlice, FxArray};

    #[test]
    fn new_fx_batches() {
        let arrays = vec![
            FxArray::from_slice(&["a", "c", "z"]).into_array(),
            FxArray::from(vec![Some("x"), None, Some("y")]).into_array(),
            FxArray::from_slice(&[true, false, false]).into_array(),
        ];
        let data = FxGrid::new(arrays);

        let batches =
            FxBundle::try_new(["c1", "c2", "c3"], data, NullableOptions::indexed_true([2]));

        assert!(batches.is_ok());

        println!("{:?}", batches.unwrap());
    }
}
