//! file: eclectic.rs
//! author: Jacob Xie
//! date: 2023/02/12 22:33:08 Sunday
//! brief: Eclectic

use arrow2::datatypes::DataType;

use crate::cont::ab::private;
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
        private::InnerEclectic::is_arr(self)
    }

    fn is_vec(&self) -> bool {
        private::InnerEclectic::is_vec(self)
    }

    fn width(&self) -> usize {
        private::InnerEclectic::width(self)
    }

    fn lens(&self) -> Vec<usize> {
        private::InnerEclectic::lens(self)
    }

    fn max_len(&self) -> Option<usize> {
        private::InnerEclectic::max_len(self)
    }

    fn min_len(&self) -> Option<usize> {
        private::InnerEclectic::min_len(self)
    }

    fn is_lens_same(&self) -> bool {
        private::InnerEclectic::is_lens_same(self)
    }

    fn is_empty(&self) -> bool {
        private::InnerEclectic::is_empty(self)
    }

    fn data_types(&self) -> Vec<&DataType> {
        private::InnerEclectic::data_types(self)
    }

    fn data_types_match<T: Eclectic>(&self, d: &T) -> bool {
        private::InnerEclectic::data_types_match(self, d)
    }

    fn sequences(&self) -> &[Self::Seq] {
        self.ref_sequences()
    }

    fn into_sequences(self) -> Vec<Self::Seq> {
        self.take_sequences()
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
