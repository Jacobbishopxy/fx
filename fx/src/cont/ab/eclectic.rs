//! file: eclectic.rs
//! author: Jacob Xie
//! date: 2023/02/12 22:33:08 Sunday
//! brief: Eclectic

use std::hash::Hash;
use std::ops::Deref;

use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;

use super::FxSeq;
use crate::ab::private;
use crate::{ArcArr, FxError, FxResult};

// ================================================================================================
// Eclectic
// ================================================================================================

/// A collection consists of several `FxSeq`s, whose inner type can be different
pub trait Eclectic: private::InnerEclectic + Sized {
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
}

impl<T> Eclectic for T where T: private::InnerEclectic {}

pub trait EclecticMutSeq: private::InnerEclecticMutSeq + Eclectic {
    fn try_extent<T: Eclectic<Seq = Self::Seq>>(&mut self, d: &T) -> FxResult<&mut Self> {
        if !Eclectic::data_types_match(self, d) {
            return Err(FxError::SchemaMismatch);
        }

        let cols = self.mut_sequences();

        let zp = cols.iter_mut().zip(d.sequences().iter());
        for (s, a) in zp {
            s.extend(a)?;
        }

        Ok(self)
    }

    fn try_concat<T: Eclectic<Seq = Self::Seq>>(&mut self, d: &[T]) -> FxResult<&mut Self> {
        for i in d.iter() {
            self.try_extent(i)?;
        }

        Ok(self)
    }
}

impl<T> EclecticMutSeq for T where T: private::InnerEclecticMutSeq {}

pub trait EclecticMutChunk: private::InnerEclecticMutChunk + Eclectic {
    fn try_extent<T: Eclectic<Seq = ArcArr>>(&mut self, d: &T) -> FxResult<&mut Self> {
        if !Eclectic::data_types_match(self, d) || !Eclectic::is_lens_same(d) {
            return Err(FxError::SchemaMismatch);
        }

        let cols = self.mut_chunk();

        let zp = cols.iter().zip(d.sequences().iter());
        let mut cct = vec![];
        for (s, a) in zp {
            let aa: ArcArr = concatenate(&[s.deref(), a.deref()])?.into();
            cct.push(aa);
        }

        *cols = Chunk::try_new(cct)?;

        Ok(self)
    }

    fn try_concat<T: Eclectic<Seq = ArcArr>>(&mut self, d: &[T]) -> FxResult<&mut Self> {
        for i in d.iter() {
            self.try_extent(i)?;
        }

        Ok(self)
    }
}

impl<T> EclecticMutChunk for T where T: private::InnerEclecticMutChunk {}

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

impl<I, C, T> EclecticCollection<I, C> for T
where
    T: private::InnerEclecticCollection<I, C>,
    I: Hash,
    C: Eclectic,
{
}
