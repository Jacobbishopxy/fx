//! file: rbatch.rs
//! author: Jacob Xie
//! date: 2023/01/20 12:36:42 Friday
//! brief: Batch

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};

use crate::cont::ab::private;
use crate::types::ArcArr;
use crate::{FxResult, NullableOptions};

#[derive(Debug, Clone)]
pub struct FxBatch {
    pub(crate) schema: Schema,
    pub(crate) data: Chunk<ArcArr>,
}

impl private::InnerEclectic for FxBatch {
    type Seq = ArcArr;

    fn empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Chunk::new(Vec::new()),
        }
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.data.arrays()
    }

    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        unimplemented!()
    }

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        self.data = Chunk::try_new(arrays)?;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.data.into_arrays()
    }
}

impl FxBatch {
    pub fn try_new<I, T>(
        fields_name: I,
        arrays: Vec<Arc<dyn Array>>,
        nullable_options: NullableOptions,
    ) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let data_types = arrays
            .iter()
            .map(|a| a.data_type())
            .cloned()
            .collect::<Vec<_>>();
        let schema = nullable_options.gen_schema(fields_name, data_types)?;

        Ok(Self {
            schema,
            data: Chunk::try_new(arrays)?,
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
mod test_batch {
    // use super::*;
    // use crate::{cont::ab::Chunking, FromSlice, FxArray};

    // #[test]
    // fn new_fx_batch_should_be_successful() {
    //     let arrays = vec![
    //         FxArray::from_slice(&["a", "c", "x"]).into_array(),
    //         FxArray::from(vec![Some("x"), None, Some("y")]).into_array(),
    //         FxArray::from_slice(&[true, false, false]).into_array(),
    //     ];

    //     let names = &["col1", "col2", "col3"];

    //     let batch = FxBatch::try_new(names, arrays, NullableOptions::indexed_true([2]));

    //     assert!(batch.is_ok());

    //     let batch = batch.unwrap();
    //     println!("{batch:?}");
    //     println!("{:?}", batch.validities());
    // }
}
