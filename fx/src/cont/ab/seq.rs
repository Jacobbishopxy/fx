//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;
use std::fmt::Debug;

use arrow2::datatypes::DataType;

use crate::cont::{ArcArr, ArcVec, BoxArr, BoxVec};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Seq
// ================================================================================================

pub trait FxSeq: Debug + Clone {
    fn new_nulls(datatype: DataType, length: usize) -> Self;

    fn new_empty(datatype: DataType) -> Self;

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

    fn to_arc_array(self) -> FxResult<ArcArr>;

    fn to_box_array(self) -> FxResult<BoxArr>;

    fn to_arc_vector(self) -> FxResult<ArcVec>;

    fn to_box_vector(self) -> FxResult<BoxVec>;

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self>;

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        for s in ss {
            Self::extend(self, s)?;
        }

        Ok(self)
    }
}
