//! file: private.rs
//! author: Jacob Xie
//! date: 2023/01/21 00:55:29 Saturday
//! brief: Private

use std::hash::Hash;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema};

use crate::cont::ab::FxSeq;
use crate::types::ArcArr;
use crate::FxResult;

// ================================================================================================
// InnerSheaf
//
// A generous purpose of Arc<dyn Array> collection.
// To replace InnerChunking.
// ================================================================================================

#[doc(hidden)]
pub trait InnerSheaf {
    type Seq: FxSeq; // Arc<Array> or Arc<MutableArray>

    fn empty() -> Self
    where
        Self: Sized;

    fn ref_sequences(&self) -> &[Self::Seq];

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>);

    fn take_sequences(self) -> Vec<Self::Seq>;

    fn take_chunk(self) -> Chunk<ArcArr>;

    // default implementations

    fn width(&self) -> usize {
        self.ref_sequences().iter().count()
    }

    fn lens(&self) -> Vec<usize> {
        self.ref_sequences()
            .iter()
            .map(|s| s.len())
            .collect::<Vec<_>>()
    }

    fn max_len(&self) -> Option<usize> {
        self.lens().iter().max().cloned()
    }

    fn min_len(&self) -> Option<usize> {
        self.lens().iter().min().cloned()
    }

    fn is_lens_same(&self) -> bool {
        let l = self.lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    fn is_empty(&self) -> bool {
        self.ref_sequences().is_empty()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.ref_sequences().iter().map(|e| e.data_type()).collect()
    }

    fn data_types_match<T: InnerSheaf>(&self, d: &T) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}

// ================================================================================================
// InnerSheafContainer
//
// Replacement of InnerChunkingContainer
// ================================================================================================

#[doc(hidden)]
pub trait InnerSheafContainer<I, C>
where
    I: Hash,
    C: InnerSheaf,
{
    fn empty() -> Self
    where
        Self: Sized;

    fn ref_schema(&self) -> &Schema;

    fn ref_container(&self) -> Vec<&C>;

    fn get_chunk(&self, key: I) -> FxResult<&C>;

    fn get_mut_chunk(&mut self, key: I) -> FxResult<&mut C>;

    fn insert_chunk_type_unchecked(&mut self, key: I, data: C) -> FxResult<()>;

    fn remove_chunk(&mut self, key: I) -> FxResult<()>;

    fn push_chunk_type_unchecked(&mut self, data: C) -> FxResult<()>;

    fn pop_chunk(&mut self) -> FxResult<()>;

    fn take_container(self) -> Vec<C>;

    // default implementations

    fn length(&self) -> usize {
        self.ref_container().len()
    }

    fn width(&self) -> usize {
        self.ref_schema().fields.len()
    }

    fn size(&self) -> (usize, usize) {
        (self.length(), self.width())
    }

    fn is_empty(&self) -> bool {
        self.ref_container().is_empty()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.ref_schema()
            .fields
            .iter()
            .map(|f| f.data_type())
            .collect::<Vec<_>>()
    }

    fn data_types_check(&self, c: &C) -> bool {
        self.width() == c.width() && self.data_types() == c.data_types()
    }

    fn data_types_match<T>(&self, d: &T) -> bool
    where
        T: InnerSheafContainer<I, C>,
    {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}

// ================================================================================================
// InnerChunking
//
// pub(crate) in mod.rs, so that external access is prohibited
// ================================================================================================

#[doc(hidden)]
pub trait InnerChunking {
    fn empty() -> Self
    where
        Self: Sized;

    fn ref_chunk(&self) -> &Chunk<Arc<dyn Array>>;

    fn set_chunk(&mut self, arrays: Vec<Arc<dyn Array>>) -> FxResult<()>;

    fn take_chunk(self) -> Chunk<Arc<dyn Array>>;

    // default implementations

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

    fn data_types(&self) -> Vec<DataType> {
        self.ref_chunk()
            .iter()
            .map(|e| e.data_type())
            .cloned()
            .collect()
    }

    fn data_types_match<T: InnerChunking>(&self, d: &T) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}

// ================================================================================================
// InnerChunkingContainer
//
// pub(crate) in mod.rs, so that external access is prohibited
// ================================================================================================

#[doc(hidden)]
pub trait InnerChunkingContainer<I, C>
where
    I: Hash,
    C: InnerChunking,
{
    fn empty() -> Self
    where
        Self: Sized;

    fn ref_schema(&self) -> &Schema;

    fn ref_container(&self) -> Vec<&C>;

    fn get_chunk(&self, key: I) -> FxResult<&C>;

    fn get_mut_chunk(&mut self, key: I) -> FxResult<&mut C>;

    fn insert_chunk_type_unchecked(&mut self, key: I, data: C) -> FxResult<()>;

    fn remove_chunk(&mut self, key: I) -> FxResult<()>;

    fn push_chunk_type_unchecked(&mut self, data: C) -> FxResult<()>;

    fn pop_chunk(&mut self) -> FxResult<()>;

    fn take_container(self) -> Vec<C>;

    // default implementations

    fn length(&self) -> usize {
        self.ref_container().len()
    }

    fn width(&self) -> usize {
        self.ref_schema().fields.len()
    }

    fn size(&self) -> (usize, usize) {
        (self.length(), self.width())
    }

    fn is_empty(&self) -> bool {
        self.ref_container().is_empty()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.ref_schema()
            .fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>()
    }

    fn data_types_check(&self, c: &C) -> bool {
        self.width() == c.width() && self.data_types() == c.data_types()
    }

    fn data_types_match<T>(&self, d: &T) -> bool
    where
        T: InnerChunkingContainer<I, C>,
    {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}
