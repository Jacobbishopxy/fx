//! file: private.rs
//! author: Jacob Xie
//! date: 2023/01/21 00:55:29 Saturday
//! brief: Private

use std::hash::Hash;

use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

use super::{Confined, FxSeq};
use crate::cont::ArcArr;
use crate::error::FxResult;

// ================================================================================================
// InnerEclectic
//
// A genetic purpose of Arc<dyn Array> collection.
// To replace InnerChunking.
// ================================================================================================

#[doc(hidden)]
pub trait InnerEclectic: Confined {
    type Seq: FxSeq; // Arc<Array>/Arc<MutableArray>/Box<Array>/Box<MutableArray>

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
// InnerReceptacle
//
// Replacement of InnerChunkingContainer
// ================================================================================================

// #[doc(hidden)]
pub trait InnerReceptacle<const SCHEMA: bool, I>
where
    I: Hash + Eq,
    Self: Sized,
{
    type In: Confined;
    type OutRef<'a>
    where
        Self: 'a;
    type OutMut<'a>
    where
        Self: 'a;

    fn new_empty() -> Self;

    fn ref_schema(&self) -> Option<&Schema>;

    fn get_chunk<'a>(&'a self, key: I) -> FxResult<Self::OutRef<'a>>;

    fn get_mut_chunk<'a>(&'a mut self, key: I) -> FxResult<Self::OutMut<'a>>;

    fn insert_chunk_type_unchecked(&mut self, key: I, data: Self::In) -> FxResult<()>;

    fn remove_chunk(&mut self, key: I) -> FxResult<()>;

    fn push_chunk_type_unchecked(&mut self, data: Self::In) -> FxResult<()>;

    fn pop_chunk(&mut self) -> FxResult<()>;
}
