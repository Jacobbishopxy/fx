//! file: receptacle.rs
//! author: Jacob Xie
//! date: 2023/03/05 00:45:02 Sunday
//! brief:

use std::hash::Hash;

use crate::ab::{private, Confined};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Receptacle
// ================================================================================================

pub trait Receptacle<const SCHEMA: bool, I, C>:
    private::InnerReceptacle<SCHEMA, I, C> + Confined
where
    I: Hash + Eq,
    C: Confined,
{
    fn new_empty() -> Self {
        private::InnerReceptacle::<SCHEMA, I, C>::new_empty()
    }

    fn length(&self) -> usize {
        self.ref_container().len()
    }

    fn size(&self) -> (usize, usize) {
        (self.length(), self.width())
    }

    fn is_empty(&self) -> bool {
        self.ref_container().is_empty()
    }

    fn data_types_check(&self, c: &C) -> bool {
        Confined::data_types_match(self, c)
    }

    fn data_types_match<T>(&self, d: &T) -> bool
    where
        T: Receptacle<SCHEMA, I, C>,
    {
        self.width() == d.width() && self.data_types() == d.data_types()
    }

    fn get(&self, key: I) -> FxResult<&C> {
        self.get_chunk(key)
    }

    fn get_mut(&mut self, key: I) -> FxResult<&mut C> {
        self.get_mut_chunk(key)
    }

    fn insert(&mut self, key: I, data: C) -> FxResult<()> {
        if SCHEMA && !self.data_types_check(&data) {
            return Err(FxError::SchemaMismatch);
        }

        self.insert_chunk_type_unchecked(key, data)
    }

    fn remove(&mut self, key: I) -> FxResult<()> {
        self.remove_chunk(key)
    }

    fn remove_many<ITR>(&mut self, keys: ITR) -> FxResult<()>
    where
        ITR: IntoIterator<Item = I>,
    {
        for i in keys {
            self.remove(i)?;
        }

        Ok(())
    }

    fn push(&mut self, data: C) -> FxResult<()> {
        if SCHEMA && !self.data_types_check(&data) {
            return Err(FxError::SchemaMismatch);
        }

        self.push_chunk_type_unchecked(data)
    }

    fn extend<ITR>(&mut self, data: ITR) -> FxResult<()>
    where
        ITR: IntoIterator<Item = C>,
    {
        for d in data {
            self.push(d)?;
        }

        Ok(())
    }

    fn pop(&mut self) -> FxResult<()> {
        self.pop_chunk()
    }

    fn into_vec_content(self) -> Vec<C> {
        self.take_container()
    }
}

impl<const SCHEMA: bool, I, C, T> Receptacle<SCHEMA, I, C> for T
where
    T: private::InnerReceptacle<SCHEMA, I, C> + Confined,
    I: Hash + Eq,
    C: Confined,
{
}