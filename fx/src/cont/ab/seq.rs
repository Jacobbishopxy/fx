//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;

use arrow2::datatypes::DataType;

use crate::cont::{ArcArr, ArcVec};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Seq
// ================================================================================================

pub trait FxSeq {
    fn new_nulls(data_type: DataType, len: usize) -> Self;

    fn is_arr() -> bool;

    fn is_vec() -> bool;

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> Option<&mut dyn Any>;

    // default impl
    fn as_typed<T: 'static>(&self) -> FxResult<&T> {
        self.as_any()
            .downcast_ref::<T>()
            .ok_or(FxError::InvalidDowncast)
    }

    // default impl
    fn as_typed_mut<T: 'static>(&mut self) -> FxResult<&mut T> {
        self.as_any_mut()
            .ok_or(FxError::InvalidDowncast)?
            .downcast_mut::<T>()
            .ok_or(FxError::InvalidDowncast)
    }

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn data_type(&self) -> &DataType;

    fn get_nulls(&self) -> Option<Vec<bool>>;

    fn has_null(&self) -> bool {
        self.get_nulls().is_some()
    }

    fn is_null(&self, idx: usize) -> Option<bool>;

    fn to_array(self) -> FxResult<ArcArr>;

    fn to_vector(self) -> FxResult<ArcVec>;

    // fn set_value<T>(&mut self, index: usize, value: T);

    // fn set_null(&mut self, index: usize);

    // fn slice_range(&self, offset: usize, length: usize) -> FxResult<&Self>;

    // fn take_range(self, offset: usize, length: usize) -> FxResult<Self>;

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self>;

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        for s in ss {
            Self::extend(self, s)?;
        }

        Ok(self)
    }
}
