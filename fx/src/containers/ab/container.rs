//! file: container.rs
//! author: Jacob Xie
//! date: 2023/01/30 10:56:05 Monday
//! brief: ChunkingContainer

use std::fmt::Debug;
use std::hash::Hash;

use arrow2::datatypes::DataType;

use crate::{private, FxError, FxResult};

pub trait ChunkingContainer<I, C>: private::InnerChunkingContainer<I, C> + Clone
where
    I: Debug + Hash,
    C: private::InnerChunking,
{
    fn empty() -> Self
    where
        Self: Sized,
    {
        private::InnerChunkingContainer::empty()
    }

    fn length(&self) -> usize {
        private::InnerChunkingContainer::length(self)
    }

    fn width(&self) -> usize {
        private::InnerChunkingContainer::width(self)
    }

    fn size(&self) -> (usize, usize) {
        private::InnerChunkingContainer::size(self)
    }

    fn is_empty(&self) -> bool {
        private::InnerChunkingContainer::is_empty(self)
    }

    fn data_types(&self) -> Vec<DataType> {
        private::InnerChunkingContainer::data_types(self)
    }

    fn data_types_match<T>(&self, d: &T) -> bool
    where
        T: ChunkingContainer<I, C>,
    {
        private::InnerChunkingContainer::data_types_match(self, d)
    }

    fn get_all(&self) -> Vec<&C> {
        self.ref_container()
    }

    fn get(&self, key: I) -> FxResult<&C> {
        self.get_chunk(key)
    }

    fn get_mut(&mut self, key: I) -> FxResult<&mut C> {
        self.get_mut_chunk(key)
    }

    fn insert(&mut self, key: I, data: C) -> FxResult<()> {
        if !self.data_types_check(&data) {
            return Err(FxError::SchemaMismatch);
        }
        self.insert_chunk_type_unchecked(key, data)
    }

    fn remove(&mut self, key: I) -> FxResult<()> {
        self.remove_chunk(key)
    }

    fn push(&mut self, data: C) -> FxResult<()> {
        if !self.data_types_check(&data) {
            return Err(FxError::SchemaMismatch);
        }
        self.push_chunk_type_unchecked(data)
    }

    fn pop(&mut self) -> FxResult<()> {
        self.pop_chunk()
    }

    fn take_all(self) -> Vec<C> {
        self.take_container()
    }

    fn slice() {
        unimplemented!()
    }

    fn append<T>(&mut self, d: T) -> FxResult<&mut Self>
    where
        T: ChunkingContainer<I, C>,
    {
        if !private::InnerChunkingContainer::data_types_match(self, &d) {
            return Err(FxError::SchemaMismatch);
        }

        for e in d.take_container().into_iter() {
            self.push_chunk_type_unchecked(e)?;
        }

        Ok(self)
    }
}

impl<T, I, C> ChunkingContainer<I, C> for T
where
    T: private::InnerChunkingContainer<I, C> + Clone,
    I: Debug + Hash,
    C: private::InnerChunking,
{
}
