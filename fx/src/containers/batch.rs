//! file: rbatch.rs
//! author: Jacob Xie
//! date: 2023/01/20 12:36:42 Friday
//! brief: Batch

use std::sync::Arc;

use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::{array::*, datatypes::Field};

use crate::{private, FxArray, FxError, FxResult};

#[derive(Debug, Clone)]
pub struct FxBatch {
    pub(crate) schema: Schema,
    pub(crate) data: Chunk<Arc<dyn Array>>,
}

impl private::InnerChunking for FxBatch {
    fn empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Chunk::new(Vec::new()),
        }
    }

    fn ref_chunk(&self) -> &Chunk<Arc<dyn Array>> {
        &self.data
    }

    fn set_chunk(&mut self, arrays: Vec<Arc<dyn Array>>) -> FxResult<()> {
        self.data = Chunk::new(arrays);
        Ok(())
    }

    fn take_chunk(self) -> Chunk<Arc<dyn Array>> {
        self.data
    }
}

impl FxBatch {
    pub fn try_new<I, T>(fields_name: I, arrays: Vec<Arc<dyn Array>>) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let iter = fields_name.into_iter();
        let (fl, al) = (iter.size_hint().0, arrays.len());
        if fl != al {
            return Err(FxError::LengthMismatch(fl, al));
        }

        let fld = iter
            .zip(arrays.iter())
            .map(|(n, a)| Field::new(n.as_ref(), a.data_type().clone(), a.null_count() > 0))
            .collect::<Vec<_>>();
        let schema = Schema::from(fld);

        Ok(Self {
            schema,
            data: Chunk::try_new(arrays)?,
        })
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_batch {
    use super::*;
    use crate::FromSlice;

    #[test]
    fn new_fx_batch_should_be_successful() {
        let arrays = vec![
            FxArray::from_slice(&["a", "c"]).into_array(),
            FxArray::from(vec![Some("x"), Some("y")]).into_array(),
            FxArray::from_slice(&[true, false]).into_array(),
        ];

        let names = &["col1", "col2", "col3"];

        let batch = FxBatch::try_new(names, arrays);

        assert!(batch.is_ok());

        println!("{:?}", batch.unwrap());
    }
}
