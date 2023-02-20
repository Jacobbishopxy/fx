//! file: private.rs
//! author: Jacob Xie
//! date: 2023/01/21 00:55:29 Saturday
//! brief: Private

use std::hash::Hash;

use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

use crate::ab::FxSeq;
use crate::{ArcArr, FxResult};

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

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()>;

    fn take_sequences(self) -> Vec<Self::Seq>;
}

#[doc(hidden)]
pub trait InnerEclecticMutSeq: InnerEclectic {
    fn mut_sequences(&mut self) -> &mut [Self::Seq];
}

#[doc(hidden)]
pub trait InnerEclecticMutChunk: InnerEclectic {
    fn mut_chunk(&mut self) -> &mut Chunk<ArcArr>;
}

// ================================================================================================
// InnerEclecticCollection
//
// Replacement of InnerChunkingContainer
// ================================================================================================

#[doc(hidden)]
pub trait InnerEclecticCollection<const SCHEMA: bool, I, C>
where
    I: Hash + Eq,
    C: InnerEclectic,
    Self: Sized,
{
    fn empty() -> Self
    where
        Self: Sized;

    fn ref_schema(&self) -> Option<&Schema>;

    fn ref_container(&self) -> Vec<&C>;

    fn get_chunk(&self, key: I) -> FxResult<&C>;

    fn get_mut_chunk(&mut self, key: I) -> FxResult<&mut C>;

    fn insert_chunk_type_unchecked(&mut self, key: I, data: C) -> FxResult<()>;

    fn remove_chunk(&mut self, key: I) -> FxResult<()>;

    fn push_chunk_type_unchecked(&mut self, data: C) -> FxResult<()>;

    fn pop_chunk(&mut self) -> FxResult<()>;

    fn take_container(self) -> Vec<C>;
}
