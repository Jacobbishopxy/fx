//! file: receptacle.rs
//! author: Jacob Xie
//! date: 2023/03/05 00:45:02 Sunday
//! brief:

use std::hash::Hash;

use crate::ab::{private, Confined, Eclectic};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Receptacle
// ================================================================================================

pub trait Receptacle<const SCHEMA: bool, I, E>:
    private::InnerReceptacle<SCHEMA, I, E> + Confined
where
    I: Hash + Eq,
    E: Eclectic + Confined,
{
    fn new_empty() -> Self {
        private::InnerReceptacle::<SCHEMA, I, E>::new_empty()
    }

    fn data_types_check<C: Confined>(&self, c: &C) -> bool {
        Confined::data_types_match(self, c)
    }

    fn data_types_match(&self, d: &E) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
    }

    fn get(&self, key: I) -> FxResult<Self::OutRef<'_>> {
        self.get_chunk(key)
    }

    fn get_mut(&mut self, key: I) -> FxResult<Self::OutMut<'_>> {
        self.get_mut_chunk(key)
    }

    fn insert(&mut self, key: I, data: E) -> FxResult<()> {
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

    fn push(&mut self, data: E) -> FxResult<()> {
        if SCHEMA && !self.data_types_check(&data) {
            return Err(FxError::SchemaMismatch);
        }

        self.push_chunk_type_unchecked(data)
    }

    fn extend<ITR>(&mut self, data: ITR) -> FxResult<()>
    where
        ITR: IntoIterator<Item = E>,
    {
        for d in data {
            self.push(d)?;
        }

        Ok(())
    }

    fn pop(&mut self) -> FxResult<()> {
        self.pop_chunk()
    }
}

impl<const SCHEMA: bool, I, E, T> Receptacle<SCHEMA, I, E> for T
where
    T: private::InnerReceptacle<SCHEMA, I, E> + Confined,
    I: Hash + Eq,
    E: Eclectic + Confined,
{
}
