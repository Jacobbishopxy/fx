//! file: private.rs
//! author: Jacob Xie
//! date: 2023/01/21 00:55:29 Saturday
//! brief: Private

use std::hash::Hash;

use arrow2::datatypes::Schema;

use crate::cont::FxSeq;
use crate::FxResult;

// ================================================================================================
// InnerEclectic
//
// A genetic purpose of Arc<dyn Array> collection.
// To replace InnerChunking.
// ================================================================================================

#[doc(hidden)]
pub trait InnerEclectic {
    type Seq: FxSeq; // Arc<Array> or Arc<MutableArray>

    fn empty() -> Self
    where
        Self: Sized;

    fn ref_sequences(&self) -> &[Self::Seq];

    // TODO: should also consider arrow's Chunk
    fn mut_sequences(&mut self) -> &mut [Self::Seq];

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()>;

    fn take_sequences(self) -> Vec<Self::Seq>;
}

// ================================================================================================
// InnerEclecticCollection
//
// Replacement of InnerChunkingContainer
// ================================================================================================

#[doc(hidden)]
pub trait InnerEclecticCollection<I, C>
where
    I: Hash,
    C: InnerEclectic,
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
}
