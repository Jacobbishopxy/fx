//! file: eclectic.rs
//! author: Jacob Xie
//! date: 2023/02/12 22:33:08 Sunday
//! brief: Eclectic

use std::hash::Hash;

use arrow2::datatypes::DataType;

use crate::cont::private;
use crate::{FxError, FxResult};

use super::FxSeq;

// ================================================================================================
// Eclectic
// ================================================================================================

/// A collection consists of several `FxSeq`s, whose inner type can be different
pub trait Eclectic: private::InnerEclectic + Clone {
    fn empty() -> Self {
        private::InnerEclectic::empty()
    }

    fn is_arr(&self) -> bool {
        Self::Seq::is_arr()
    }

    fn is_vec(&self) -> bool {
        Self::Seq::is_vec()
    }

    fn width(&self) -> usize {
        self.ref_sequences().iter().count()
    }

    fn lens(&self) -> Vec<usize> {
        self.ref_sequences()
            .iter()
            .map(|s| s.len())
            .collect::<Vec<_>>()
    }

    // if `lens()` is empty, return `None`
    fn max_len(&self) -> Option<usize> {
        self.lens().iter().max().cloned()
    }

    // if `lens()` is empty, return `None`
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

    fn data_types_match<T: Eclectic>(&self, d: &T) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
    }

    fn sequences(&self) -> &[Self::Seq] {
        self.ref_sequences()
    }

    fn into_sequences(self) -> Vec<Self::Seq> {
        self.take_sequences()
    }

    fn check_nulls(&self) -> Vec<bool> {
        self.ref_sequences().iter().map(|s| s.has_null()).collect()
    }

    fn slice() {
        unimplemented!()
    }

    fn try_extent<T: Eclectic<Seq = Self::Seq>>(&mut self, d: &T) -> FxResult<&mut Self> {
        self.try_concat(&[d.clone()])
    }

    // TODO: it requires `self.mut_sequences()`! However, if a struct's field is arrow's `Chunk`, it is failed
    fn try_concat<T: Eclectic<Seq = Self::Seq>>(&mut self, d: &[T]) -> FxResult<&mut Self> {
        for e in d.iter() {
            if !Eclectic::data_types_match(self, e) {
                return Err(FxError::SchemaMismatch);
            }
        }

        let cols = self.mut_sequences();

        for sheaf in d.iter() {
            let zp = cols.iter_mut().zip(sheaf.sequences().iter());
            for (s, a) in zp {
                s.extend(a)?;
            }
        }

        Ok(self)
    }
}

impl<T> Eclectic for T where T: private::InnerEclectic + Clone {}

// ================================================================================================
// EclecticCollection
// ================================================================================================

pub trait EclecticCollection<I, C>: private::InnerEclecticCollection<I, C>
where
    I: Hash,
    C: Eclectic,
{
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
        T: EclecticCollection<I, C>,
    {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}
