//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;
use std::fmt::Debug;

use arrow2::array::Array;
use arrow2::datatypes::DataType;

use crate::cont::{ArcArr, ArcVec, BoxArr, BoxVec};
use crate::error::{FxError, FxResult};
use crate::types::*;

// ================================================================================================
// Seq
// ================================================================================================

pub trait FxSeq: Debug {
    fn from_ref(data: &dyn Array) -> Self;

    fn from_box_arr(data: Box<dyn Array>) -> Self;

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

// ================================================================================================
// AsArray
// ================================================================================================

pub trait AsArray {
    fn as_bool_arr_unchecked(&self) -> &BA;
    fn as_i8_arr_unchecked(&self) -> &PAi8;
    fn as_i16_arr_unchecked(&self) -> &PAi16;
    fn as_i32_arr_unchecked(&self) -> &PAi32;
    fn as_i64_arr_unchecked(&self) -> &PAi64;
    fn as_u8_arr_unchecked(&self) -> &PAu8;
    fn as_u16_arr_unchecked(&self) -> &PAu16;
    fn as_u32_arr_unchecked(&self) -> &PAu32;
    fn as_u64_arr_unchecked(&self) -> &PAu64;
    fn as_f32_arr_unchecked(&self) -> &PAf32;
    fn as_f64_arr_unchecked(&self) -> &PAf64;
    fn as_str_arr_unchecked(&self) -> &UA;
}

// ================================================================================================
// AsVector (MutableArray)
// ================================================================================================

pub trait AsVector {
    fn as_bool_vec_unchecked(&self) -> &BV;
    fn as_i8_vec_unchecked(&self) -> &PVi8;
    fn as_i16_vec_unchecked(&self) -> &PVi16;
    fn as_i32_vec_unchecked(&self) -> &PVi32;
    fn as_i64_vec_unchecked(&self) -> &PVi64;
    fn as_u8_vec_unchecked(&self) -> &PVu8;
    fn as_u16_vec_unchecked(&self) -> &PVu16;
    fn as_u32_vec_unchecked(&self) -> &PVu32;
    fn as_u64_vec_unchecked(&self) -> &PVu64;
    fn as_f32_vec_unchecked(&self) -> &PVf32;
    fn as_f64_vec_unchecked(&self) -> &PVf64;
    fn as_str_vec_unchecked(&self) -> &UV;
}
