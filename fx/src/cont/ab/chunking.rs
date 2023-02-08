//! file: chunking.rs
//! author: Jacob Xie
//! date: 2023/01/20 23:59:13 Friday
//! brief: Chunking

use std::{ops::Deref, sync::Arc};

use arrow2::array::Array;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;

use crate::cont::ab::private;
use crate::{FxError, FxResult};

pub trait Chunking: private::InnerChunking + Clone {
    fn empty() -> Self {
        private::InnerChunking::empty()
    }

    fn length(&self) -> usize {
        private::InnerChunking::length(self)
    }

    fn width(&self) -> usize {
        private::InnerChunking::width(self)
    }

    fn size(&self) -> (usize, usize) {
        private::InnerChunking::size(self)
    }

    fn is_empty(&self) -> bool {
        private::InnerChunking::is_empty(self)
    }

    fn data_types(&self) -> Vec<DataType> {
        private::InnerChunking::data_types(self)
    }

    fn data_types_match<T: Chunking>(&self, d: &T) -> bool {
        private::InnerChunking::data_types_match(self, d)
    }

    fn validities(&self) -> Vec<bool> {
        self.ref_chunk()
            .arrays()
            .iter()
            .map(|a| a.validity().is_none())
            .collect()
    }

    fn arrays(&self) -> &[Arc<dyn Array>] {
        self.ref_chunk().arrays()
    }

    fn into_arrays(self) -> Vec<Arc<dyn Array>> {
        self.take_chunk().into_arrays()
    }

    fn slice() {
        unimplemented!()
    }

    fn extend<T: Chunking>(&mut self, d: &T) -> FxResult<&mut Self> {
        self.concat(&[d.clone()])
    }

    fn concat<T: Chunking>(&mut self, d: &[T]) -> FxResult<&mut Self> {
        // check schema integrity
        for e in d.iter() {
            if !private::InnerChunking::data_types_match(self, e) {
                return Err(FxError::SchemaMismatch);
            }
        }

        // original data as column, lift each column into a vector
        // in order to store further column from the input data
        let mut cols = self
            .arrays()
            .iter()
            .map(|e| vec![e.deref()])
            .collect::<Vec<_>>();

        // iterate through input data
        for chunk in d.iter() {
            // mutate columns by appending chunk columns
            cols.iter_mut()
                .zip(chunk.arrays().iter())
                .for_each(|(c, e)| c.push(e.deref()));
        }

        // concatenate each columns
        let mut concated = Vec::<Arc<dyn Array>>::new();
        for c in cols {
            concated.push(Arc::from(concatenate(&c)?));
        }

        self.set_chunk(concated)?;

        Ok(self)
    }
}

impl<T> Chunking for T where T: private::InnerChunking + Clone {}
