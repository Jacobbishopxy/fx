//! file: sheaf.rs
//! author: Jacob Xie
//! date: 2023/02/12 22:33:08 Sunday
//! brief: Sheaf & Container

use arrow2::{chunk::Chunk, datatypes::DataType};

use crate::cont::ab::private;
use crate::types::ArcArr;
use crate::{FxError, FxResult};

use super::FxSeq;

pub trait Sheaf: private::InnerSheaf + Clone {
    fn empty() -> Self {
        private::InnerSheaf::empty()
    }

    fn width(&self) -> usize {
        private::InnerSheaf::width(self)
    }

    fn lens(&self) -> Vec<usize> {
        private::InnerSheaf::lens(self)
    }

    fn max_len(&self) -> Option<usize> {
        private::InnerSheaf::max_len(self)
    }

    fn min_len(&self) -> Option<usize> {
        private::InnerSheaf::min_len(self)
    }

    fn is_lens_same(&self) -> bool {
        private::InnerSheaf::is_lens_same(self)
    }

    fn is_empty(&self) -> bool {
        private::InnerSheaf::is_empty(self)
    }

    fn data_types(&self) -> Vec<&DataType> {
        private::InnerSheaf::data_types(self)
    }

    fn data_types_match<T: Sheaf>(&self, d: &T) -> bool {
        private::InnerSheaf::data_types_match(self, d)
    }

    fn sequences(&self) -> &[Self::Seq] {
        self.ref_sequences()
    }

    fn into_sequences(self) -> Vec<Self::Seq> {
        self.take_sequences()
    }

    fn into_chunk(self) -> Chunk<ArcArr> {
        self.take_chunk()
    }

    fn slice() {
        unimplemented!()
    }

    fn extent<T: Sheaf<Seq = Self::Seq>>(&mut self, d: &T) -> FxResult<&mut Self> {
        self.concat(&[d.clone()])
    }

    fn concat<T: Sheaf<Seq = Self::Seq>>(&mut self, d: &[T]) -> FxResult<&mut Self> {
        for e in d.iter() {
            if !Sheaf::data_types_match(self, e) {
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

impl<T> Sheaf for T where T: private::InnerSheaf + Clone {}
