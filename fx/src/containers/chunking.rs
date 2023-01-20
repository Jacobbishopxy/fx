//! file: chunking.rs
//! author: Jacob Xie
//! date: 2023/01/20 23:59:13 Friday
//! brief: Chunking

use std::{ops::Deref, sync::Arc};

use arrow2::array::Array;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;

use crate::{private, FxError, FxResult};

pub trait Chunking: private::InnerChunking + Clone {
    fn empty() -> Self {
        private::InnerChunking::empty()
    }

    // WARNING: arrays with different length will cause runtime panic!!!
    fn new(arrays: Vec<Arc<dyn Array>>) -> Self {
        private::InnerChunking::new(arrays)
    }

    fn try_new(arrays: Vec<Arc<dyn Array>>) -> FxResult<Self> {
        private::InnerChunking::try_new(arrays)
    }

    fn length(&self) -> usize {
        self.ref_chunk().len()
    }

    fn width(&self) -> usize {
        self.ref_chunk().iter().count()
    }

    fn size(&self) -> (usize, usize) {
        (self.length(), self.width())
    }

    fn is_empty(&self) -> bool {
        self.ref_chunk().is_empty()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.ref_chunk().iter().map(|e| e.data_type()).collect()
    }

    fn data_types_match<T: Chunking>(&self, d: &T) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
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
            if !self.data_types_match(e) {
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

pub trait ChunkingContainer {
    type UnitData: Chunking;
    type Data: IntoIterator<Item = Self::UnitData>;
}

// TODO
