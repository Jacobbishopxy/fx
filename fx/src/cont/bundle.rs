//! file: bundle.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Bundle

use arrow2::datatypes::{DataType, Field, Schema};

use crate::cont::ab::{private, Chunking};
use crate::{FxError, FxResult, NullableOptions};

#[derive(Debug, Clone)]
pub struct FxBundle<T: Chunking> {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<T>,
}

impl<T> Default for FxBundle<T>
where
    T: Chunking,
{
    fn default() -> Self {
        Self {
            schema: Default::default(),
            data: Vec::<T>::new(),
        }
    }
}

impl<T: Chunking> private::InnerChunkingContainer<usize, T> for FxBundle<T> {
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::new(),
        }
    }

    fn ref_schema(&self) -> &Schema {
        &self.schema
    }

    fn ref_container(&self) -> Vec<&T> {
        self.data.iter().collect()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&T> {
        self.data
            .get(key)
            .ok_or_else(|| FxError::LengthMismatch(key, self.data.len()))
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut T> {
        let s_len = self.data.len();
        self.data
            .get_mut(key)
            .ok_or_else(|| FxError::LengthMismatch(key, s_len))
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: T) -> FxResult<()> {
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

    fn push_chunk_type_unchecked(&mut self, data: T) -> FxResult<()> {
        self.data.push(data);
        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop();
        Ok(())
    }

    fn take_container(self) -> Vec<T> {
        self.data
    }
}

impl<C: Chunking> FxBundle<C> {
    pub fn try_new<I, T>(
        fields_name: I,
        data: C,
        nullable_options: NullableOptions,
    ) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let schema = nullable_options.gen_schema(fields_name, Chunking::data_types(&data))?;

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

    pub fn new_empty_by_fields<I, F>(fields: I) -> FxResult<Self>
    where
        I: IntoIterator<Item = F>,
        F: Into<Field>,
    {
        let schema = Schema::from(fields.into_iter().map(|f| f.into()).collect::<Vec<_>>());

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
mod test_bundle {
    // use super::*;

    // use crate::cont::ab::*;
    // use crate::{FromSlice, FxArray, FxGrid};

    // #[test]
    // fn new_fx_batches() {
    //     let arrays = vec![
    //         FxArray::from_slice(&["a", "c", "z"]).into_array(),
    //         FxArray::from(vec![Some("x"), None, Some("y")]).into_array(),
    //         FxArray::from_slice(&[true, false, false]).into_array(),
    //     ];
    //     let data = FxGrid::new(arrays);

    //     let batches =
    //         FxBundle::try_new(["c1", "c2", "c3"], data, NullableOptions::indexed_true([2]));

    //     assert!(batches.is_ok());

    //     println!("{:?}", batches.unwrap());
    // }

    // #[test]
    // fn grid_builder_row_wise_proc_macro_success() {
    //     use crate::FX;

    //     #[allow(dead_code)]
    //     #[derive(FX)]
    //     struct Users {
    //         id: i32,
    //         name: String,
    //         check: Option<bool>,
    //     }

    //     let r1 = Users {
    //         id: 1,
    //         name: "Jacob".to_string(),
    //         check: Some(true),
    //     };

    //     let r2 = Users {
    //         id: 2,
    //         name: "Mia".to_string(),
    //         check: None,
    //     };

    //     let mut bd = Users::gen_container_row_builder().unwrap();

    //     bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

    //     let d = bd.build();

    //     println!("{d:?}");
    // }
}
