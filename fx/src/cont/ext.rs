//! file: ext.rs
//! author: Jacob Xie
//! date: 2023/02/20 20:09:06 Monday
//! brief: Arrow extensions

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

// use arrow2::array::TryPush;
use arrow2::array::{Array, MutableArray, TryExtendFromSelf};
use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Schema};

use crate::ab::*;
use crate::cont::private::macros::*;
use crate::error::{FxError, FxResult};
use crate::types::*;

// ================================================================================================
// Arrow types reexport
// ================================================================================================

pub type ArcArr = Arc<dyn Array>;
pub type BoxArr = Box<dyn Array>;
pub type ArcVec = Arc<dyn MutableArray>;
pub type BoxVec = Box<dyn MutableArray>;
pub type ChunkArr = Chunk<ArcArr>;

// ================================================================================================
// Arc<dyn Array>
// ================================================================================================

impl FxSeq for ArcArr {
    fn from_ref(data: &dyn Array) -> Self {
        Arc::from(data.to_boxed())
    }

    fn from_box_arr(data: Box<dyn Array>) -> Self {
        Arc::from(data)
    }

    fn new_nulls(data_type: DataType, length: usize) -> Self {
        match data_type {
            DataType::Boolean => BA::new_null(data_type, length).arced(),
            DataType::Int8 => PAi8::new_null(data_type, length).arced(),
            DataType::Int16 => PAi16::new_null(data_type, length).arced(),
            DataType::Int32 => PAi32::new_null(data_type, length).arced(),
            DataType::Int64 => PAi64::new_null(data_type, length).arced(),
            DataType::UInt8 => PAu8::new_null(data_type, length).arced(),
            DataType::UInt16 => PAu16::new_null(data_type, length).arced(),
            DataType::UInt32 => PAu32::new_null(data_type, length).arced(),
            DataType::UInt64 => PAu64::new_null(data_type, length).arced(),
            DataType::Float32 => PAf32::new_null(data_type, length).arced(),
            DataType::Float64 => PAf64::new_null(data_type, length).arced(),
            DataType::Utf8 => UA::new_null(data_type, length).arced(),
            DataType::Null => NA::new_null(data_type, length).arced(),
            _ => unimplemented!(),
        }
    }

    fn new_empty(data_type: DataType) -> Self {
        Self::new_nulls(data_type, 0)
    }

    fn is_arr() -> bool {
        true
    }

    fn is_vec() -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_any_mut())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_validity(&self) -> Option<Vec<bool>> {
        self.validity().map(|bm| bm.iter().collect())
    }

    fn is_valid(&self, idx: usize) -> Option<bool> {
        self.get_validity().and_then(|e| e.get(idx).copied())
    }

    fn to_arc_array(self) -> FxResult<ArcArr> {
        Ok(self)
    }

    fn to_box_array(self) -> FxResult<BoxArr> {
        Ok(self.to_boxed())
    }

    fn to_arc_vector(self) -> FxResult<ArcVec> {
        match &self.data_type() {
            DataType::Boolean => arc_arr_to_vec!(self, BA, BV),
            DataType::Int8 => arc_arr_to_vec_p!(self, PAi8, PVi8),
            DataType::Int16 => arc_arr_to_vec_p!(self, PAi16, PVi16),
            DataType::Int32 => arc_arr_to_vec_p!(self, PAi32, PVi32),
            DataType::Int64 => arc_arr_to_vec_p!(self, PAi64, PVi64),
            DataType::UInt8 => arc_arr_to_vec_p!(self, PAu8, PVu8),
            DataType::UInt16 => arc_arr_to_vec_p!(self, PAu16, PVu16),
            DataType::UInt32 => arc_arr_to_vec_p!(self, PAu32, PVu32),
            DataType::UInt64 => arc_arr_to_vec_p!(self, PAu64, PVu64),
            DataType::Float32 => arc_arr_to_vec_p!(self, PAf32, PVf32),
            DataType::Float64 => arc_arr_to_vec_p!(self, PAf64, PVf64),
            DataType::Utf8 => arc_arr_to_vec!(self, UA, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn to_box_vector(self) -> FxResult<BoxVec> {
        match &self.data_type() {
            DataType::Boolean => box_arr_to_vec!(self, BA, BV),
            DataType::Int8 => box_arr_to_vec_p!(self, PAi8, PVi8),
            DataType::Int16 => box_arr_to_vec_p!(self, PAi16, PVi16),
            DataType::Int32 => box_arr_to_vec_p!(self, PAi32, PVi32),
            DataType::Int64 => box_arr_to_vec_p!(self, PAi64, PVi64),
            DataType::UInt8 => box_arr_to_vec_p!(self, PAu8, PVu8),
            DataType::UInt16 => box_arr_to_vec_p!(self, PAu16, PVu16),
            DataType::UInt32 => box_arr_to_vec_p!(self, PAu32, PVu32),
            DataType::UInt64 => box_arr_to_vec_p!(self, PAu64, PVu64),
            DataType::Float32 => box_arr_to_vec_p!(self, PAf32, PVf32),
            DataType::Float64 => box_arr_to_vec_p!(self, PAf64, PVf64),
            DataType::Utf8 => box_arr_to_vec!(self, UA, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn extend(&mut self, s: &ArcArr) -> FxResult<&mut Self> {
        let ct = concatenate(&[self.as_ref(), s.deref()])?;
        *self = Arc::from(ct);

        Ok(self)
    }

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        let mut ars = vec![self.as_ref()];
        let ss_d = ss.iter().map(|s| (*s).deref()).collect::<Vec<_>>();
        ars.extend_from_slice(&ss_d);

        let ct = concatenate(&ars)?;
        *self = Arc::from(ct);

        Ok(self)
    }
}

impl AsArray for ArcArr {
    fn as_bool_arr_unchecked(&self) -> &BA {
        self.as_typed::<BA>().expect("BA")
    }

    fn as_i8_arr_unchecked(&self) -> &PAi8 {
        self.as_typed::<PAi8>().expect("PAi8")
    }

    fn as_i16_arr_unchecked(&self) -> &PAi16 {
        self.as_typed::<PAi16>().expect("PAi16")
    }

    fn as_i32_arr_unchecked(&self) -> &PAi32 {
        self.as_typed::<PAi32>().expect("PAi32")
    }

    fn as_i64_arr_unchecked(&self) -> &PAi64 {
        self.as_typed::<PAi64>().expect("PAi64")
    }

    fn as_u8_arr_unchecked(&self) -> &PAu8 {
        self.as_typed::<PAu8>().expect("PAu8")
    }

    fn as_u16_arr_unchecked(&self) -> &PAu16 {
        self.as_typed::<PAu16>().expect("PAu16")
    }

    fn as_u32_arr_unchecked(&self) -> &PAu32 {
        self.as_typed::<PAu32>().expect("PAu32")
    }

    fn as_u64_arr_unchecked(&self) -> &PAu64 {
        self.as_typed::<PAu64>().expect("PAu64")
    }

    fn as_f32_arr_unchecked(&self) -> &PAf32 {
        self.as_typed::<PAf32>().expect("PAf32")
    }

    fn as_f64_arr_unchecked(&self) -> &PAf64 {
        self.as_typed::<PAf64>().expect("PAf64")
    }

    fn as_str_arr_unchecked(&self) -> &UA {
        self.as_typed::<UA>().expect("UA")
    }
}

// ================================================================================================
// Box<dyn Array>
// ================================================================================================

impl FxSeq for BoxArr {
    fn from_ref(data: &dyn Array) -> Self {
        data.to_boxed()
    }

    fn from_box_arr(data: Box<dyn Array>) -> Self {
        data
    }

    fn new_nulls(data_type: DataType, length: usize) -> Self {
        match data_type {
            DataType::Boolean => BA::new_null(data_type, length).boxed(),
            DataType::Int8 => PAi8::new_null(data_type, length).boxed(),
            DataType::Int16 => PAi16::new_null(data_type, length).boxed(),
            DataType::Int32 => PAi32::new_null(data_type, length).boxed(),
            DataType::Int64 => PAi64::new_null(data_type, length).boxed(),
            DataType::UInt8 => PAu8::new_null(data_type, length).boxed(),
            DataType::UInt16 => PAu16::new_null(data_type, length).boxed(),
            DataType::UInt32 => PAu32::new_null(data_type, length).boxed(),
            DataType::UInt64 => PAu64::new_null(data_type, length).boxed(),
            DataType::Float32 => PAf32::new_null(data_type, length).boxed(),
            DataType::Float64 => PAf64::new_null(data_type, length).boxed(),
            DataType::Utf8 => UA::new_null(data_type, length).boxed(),
            DataType::Null => NA::new_null(data_type, length).boxed(),
            _ => unimplemented!(),
        }
    }

    fn new_empty(data_type: DataType) -> Self {
        Self::new_nulls(data_type, 0)
    }

    fn is_arr() -> bool {
        true
    }

    fn is_vec() -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Some((**self).as_any_mut())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_validity(&self) -> Option<Vec<bool>> {
        self.validity().map(|bm| bm.iter().collect())
    }

    fn is_valid(&self, idx: usize) -> Option<bool> {
        self.get_validity().and_then(|e| e.get(idx).copied())
    }

    fn to_arc_array(self) -> FxResult<ArcArr> {
        Ok(Arc::from(self))
    }

    fn to_box_array(self) -> FxResult<BoxArr> {
        Ok(self)
    }

    fn to_arc_vector(self) -> FxResult<ArcVec> {
        match &self.data_type() {
            DataType::Boolean => arc_arr_to_vec!(self, BA, BV),
            DataType::Int8 => arc_arr_to_vec_p!(self, PAi8, PVi8),
            DataType::Int16 => arc_arr_to_vec_p!(self, PAi16, PVi16),
            DataType::Int32 => arc_arr_to_vec_p!(self, PAi32, PVi32),
            DataType::Int64 => arc_arr_to_vec_p!(self, PAi64, PVi64),
            DataType::UInt8 => arc_arr_to_vec_p!(self, PAu8, PVu8),
            DataType::UInt16 => arc_arr_to_vec_p!(self, PAu16, PVu16),
            DataType::UInt32 => arc_arr_to_vec_p!(self, PAu32, PVu32),
            DataType::UInt64 => arc_arr_to_vec_p!(self, PAu64, PVu64),
            DataType::Float32 => arc_arr_to_vec_p!(self, PAf32, PVf32),
            DataType::Float64 => arc_arr_to_vec_p!(self, PAf64, PVf64),
            DataType::Utf8 => arc_arr_to_vec!(self, UA, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn to_box_vector(self) -> FxResult<BoxVec> {
        match &self.data_type() {
            DataType::Boolean => box_arr_to_vec!(self, BA, BV),
            DataType::Int8 => box_arr_to_vec_p!(self, PAi8, PVi8),
            DataType::Int16 => box_arr_to_vec_p!(self, PAi16, PVi16),
            DataType::Int32 => box_arr_to_vec_p!(self, PAi32, PVi32),
            DataType::Int64 => box_arr_to_vec_p!(self, PAi64, PVi64),
            DataType::UInt8 => box_arr_to_vec_p!(self, PAu8, PVu8),
            DataType::UInt16 => box_arr_to_vec_p!(self, PAu16, PVu16),
            DataType::UInt32 => box_arr_to_vec_p!(self, PAu32, PVu32),
            DataType::UInt64 => box_arr_to_vec_p!(self, PAu64, PVu64),
            DataType::Float32 => box_arr_to_vec_p!(self, PAf32, PVf32),
            DataType::Float64 => box_arr_to_vec_p!(self, PAf64, PVf64),
            DataType::Utf8 => box_arr_to_vec!(self, UA, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self> {
        let ct = concatenate(&[self.as_ref(), s.deref()])?;
        *self = ct;

        Ok(self)
    }

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        let mut ars = vec![self.as_ref()];
        let ss_d = ss.iter().map(|s| (*s).deref()).collect::<Vec<_>>();
        ars.extend_from_slice(&ss_d);

        let ct = concatenate(&ars)?;
        *self = ct;

        Ok(self)
    }
}

impl AsArray for BoxArr {
    fn as_bool_arr_unchecked(&self) -> &BA {
        self.as_typed::<BA>().expect("BA")
    }

    fn as_i8_arr_unchecked(&self) -> &PAi8 {
        self.as_typed::<PAi8>().expect("PAi8")
    }

    fn as_i16_arr_unchecked(&self) -> &PAi16 {
        self.as_typed::<PAi16>().expect("PAi16")
    }

    fn as_i32_arr_unchecked(&self) -> &PAi32 {
        self.as_typed::<PAi32>().expect("PAi32")
    }

    fn as_i64_arr_unchecked(&self) -> &PAi64 {
        self.as_typed::<PAi64>().expect("PAi64")
    }

    fn as_u8_arr_unchecked(&self) -> &PAu8 {
        self.as_typed::<PAu8>().expect("PAu8")
    }

    fn as_u16_arr_unchecked(&self) -> &PAu16 {
        self.as_typed::<PAu16>().expect("PAu16")
    }

    fn as_u32_arr_unchecked(&self) -> &PAu32 {
        self.as_typed::<PAu32>().expect("PAu32")
    }

    fn as_u64_arr_unchecked(&self) -> &PAu64 {
        self.as_typed::<PAu64>().expect("PAu64")
    }

    fn as_f32_arr_unchecked(&self) -> &PAf32 {
        self.as_typed::<PAf32>().expect("PAf32")
    }

    fn as_f64_arr_unchecked(&self) -> &PAf64 {
        self.as_typed::<PAf64>().expect("PAf64")
    }

    fn as_str_arr_unchecked(&self) -> &UA {
        self.as_typed::<UA>().expect("UA")
    }
}

// ================================================================================================
// Arc<dyn MutableArray>
// ================================================================================================

impl FxSeq for ArcVec {
    fn from_ref(data: &dyn Array) -> Self {
        data.to_boxed().to_arc_vector().unwrap()
    }

    fn from_box_arr(data: Box<dyn Array>) -> Self {
        data.to_arc_vector().unwrap()
    }

    fn new_nulls(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::Boolean => Arc::new(BV::from(vec![None; len])),
            DataType::Int8 => Arc::new(PVi8::from(vec![None; len])),
            DataType::Int16 => Arc::new(PVi16::from(vec![None; len])),
            DataType::Int32 => Arc::new(PVi32::from(vec![None; len])),
            DataType::Int64 => Arc::new(PVi64::from(vec![None; len])),
            DataType::UInt8 => Arc::new(PVu8::from(vec![None; len])),
            DataType::UInt16 => Arc::new(PVu16::from(vec![None; len])),
            DataType::UInt32 => Arc::new(PVu32::from(vec![None; len])),
            DataType::UInt64 => Arc::new(PVu64::from(vec![None; len])),
            DataType::Float32 => Arc::new(PVf32::from(vec![None; len])),
            DataType::Float64 => Arc::new(PVf64::from(vec![None; len])),
            DataType::Utf8 => Arc::new(UV::from(vec![Option::<&str>::None; len])),
            _ => unimplemented!(),
        }
    }

    fn new_empty(data_type: DataType) -> Self {
        Self::new_nulls(data_type, 0)
    }

    fn is_arr() -> bool {
        false
    }

    fn is_vec() -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_mut_any())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_validity(&self) -> Option<Vec<bool>> {
        self.validity().map(|bm| bm.iter().collect())
    }

    fn is_valid(&self, idx: usize) -> Option<bool> {
        self.get_validity().and_then(|e| e.get(idx).copied())
    }

    // If atomically reference count (Arc) > 1, then return `FxError::FailedToConvert`
    fn to_arc_array(mut self) -> FxResult<ArcArr> {
        let res = Arc::get_mut(&mut self)
            .ok_or(FxError::FailedToConvert)?
            .as_arc();

        Ok(res)
    }

    // If atomically reference count (Arc) > 1, then return `FxError::FailedToConvert`
    fn to_box_array(mut self) -> FxResult<BoxArr> {
        let res = Arc::get_mut(&mut self)
            .ok_or(FxError::FailedToConvert)?
            .as_box();

        Ok(res)
    }

    fn to_arc_vector(self) -> FxResult<ArcVec> {
        Ok(self)
    }

    fn to_box_vector(self) -> FxResult<BoxVec> {
        self.to_box_array()?.to_box_vector()
    }

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self> {
        match &self.data_type() {
            DataType::Boolean => try_ext_from_slf!(self, s, BV),
            DataType::Int8 => try_ext_from_slf!(self, s, PVi8),
            DataType::Int16 => try_ext_from_slf!(self, s, PVi16),
            DataType::Int32 => try_ext_from_slf!(self, s, PVi32),
            DataType::Int64 => try_ext_from_slf!(self, s, PVi64),
            DataType::UInt8 => try_ext_from_slf!(self, s, PVu8),
            DataType::UInt16 => try_ext_from_slf!(self, s, PVu16),
            DataType::UInt32 => try_ext_from_slf!(self, s, PVu32),
            DataType::UInt64 => try_ext_from_slf!(self, s, PVu64),
            DataType::Float32 => try_ext_from_slf!(self, s, PVf32),
            DataType::Float64 => try_ext_from_slf!(self, s, PVf64),
            DataType::Utf8 => try_ext_from_slf!(self, s, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }
}

impl AsVector for ArcVec {
    fn as_bool_vec_unchecked(&self) -> &BV {
        self.as_typed::<BV>().expect("BV")
    }

    fn as_i8_vec_unchecked(&self) -> &PVi8 {
        self.as_typed::<PVi8>().expect("PVi8")
    }

    fn as_i16_vec_unchecked(&self) -> &PVi16 {
        self.as_typed::<PVi16>().expect("PVi16")
    }

    fn as_i32_vec_unchecked(&self) -> &PVi32 {
        self.as_typed::<PVi32>().expect("PVi32")
    }

    fn as_i64_vec_unchecked(&self) -> &PVi64 {
        self.as_typed::<PVi64>().expect("PVi64")
    }

    fn as_u8_vec_unchecked(&self) -> &PVu8 {
        self.as_typed::<PVu8>().expect("PVu8")
    }

    fn as_u16_vec_unchecked(&self) -> &PVu16 {
        self.as_typed::<PVu16>().expect("PVu16")
    }

    fn as_u32_vec_unchecked(&self) -> &PVu32 {
        self.as_typed::<PVu32>().expect("PVu32")
    }

    fn as_u64_vec_unchecked(&self) -> &PVu64 {
        self.as_typed::<PVu64>().expect("PVu64")
    }

    fn as_f32_vec_unchecked(&self) -> &PVf32 {
        self.as_typed::<PVf32>().expect("PVf32")
    }

    fn as_f64_vec_unchecked(&self) -> &PVf64 {
        self.as_typed::<PVf64>().expect("PVf64")
    }

    fn as_str_vec_unchecked(&self) -> &UV {
        self.as_typed::<UV>().expect("UV")
    }
}

// ================================================================================================
// Box<dyn MutableArray>
// ================================================================================================

impl FxSeq for BoxVec {
    fn from_ref(data: &dyn Array) -> Self {
        data.to_boxed().to_box_vector().unwrap()
    }

    fn from_box_arr(data: Box<dyn Array>) -> Self {
        data.to_box_vector().unwrap()
    }

    fn new_nulls(data_type: DataType, length: usize) -> Self {
        match data_type {
            DataType::Boolean => Box::new(BV::from(vec![None; length])),
            DataType::Int8 => Box::new(PVi8::from(vec![None; length])),
            DataType::Int16 => Box::new(PVi16::from(vec![None; length])),
            DataType::Int32 => Box::new(PVi32::from(vec![None; length])),
            DataType::Int64 => Box::new(PVi64::from(vec![None; length])),
            DataType::UInt8 => Box::new(PVu8::from(vec![None; length])),
            DataType::UInt16 => Box::new(PVu16::from(vec![None; length])),
            DataType::UInt32 => Box::new(PVu32::from(vec![None; length])),
            DataType::UInt64 => Box::new(PVu64::from(vec![None; length])),
            DataType::Float32 => Box::new(PVf32::from(vec![None; length])),
            DataType::Float64 => Box::new(PVf64::from(vec![None; length])),
            DataType::Utf8 => Box::new(UV::from(vec![Option::<&str>::None; length])),
            _ => unimplemented!(),
        }
    }

    fn new_empty(data_type: DataType) -> Self {
        Self::new_nulls(data_type, 0)
    }

    fn is_arr() -> bool {
        false
    }

    fn is_vec() -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Some((**self).as_mut_any())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_validity(&self) -> Option<Vec<bool>> {
        self.validity().map(|bm| bm.iter().collect())
    }

    fn is_valid(&self, idx: usize) -> Option<bool> {
        self.get_validity().and_then(|e| e.get(idx).copied())
    }

    fn to_arc_array(mut self) -> FxResult<ArcArr> {
        let res = self.as_arc();
        Ok(res)
    }

    fn to_box_array(mut self) -> FxResult<BoxArr> {
        Ok(self.as_box())
    }

    fn to_arc_vector(self) -> FxResult<ArcVec> {
        Ok(Arc::from(self))
    }

    fn to_box_vector(self) -> FxResult<BoxVec> {
        Ok(self)
    }

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self> {
        match (**self).data_type() {
            DataType::Boolean => try_ext_from_slf!(self, s, BV),
            DataType::Int8 => try_ext_from_slf!(self, s, PVi8),
            DataType::Int16 => try_ext_from_slf!(self, s, PVi16),
            DataType::Int32 => try_ext_from_slf!(self, s, PVi32),
            DataType::Int64 => try_ext_from_slf!(self, s, PVi64),
            DataType::UInt8 => try_ext_from_slf!(self, s, PVu8),
            DataType::UInt16 => try_ext_from_slf!(self, s, PVu16),
            DataType::UInt32 => try_ext_from_slf!(self, s, PVu32),
            DataType::UInt64 => try_ext_from_slf!(self, s, PVu64),
            DataType::Float32 => try_ext_from_slf!(self, s, PVf32),
            DataType::Float64 => try_ext_from_slf!(self, s, PVf64),
            DataType::Utf8 => try_ext_from_slf!(self, s, UV),
            _ => Err(FxError::FailedToConvert),
        }
    }
}

impl AsVector for BoxVec {
    fn as_bool_vec_unchecked(&self) -> &BV {
        self.as_typed::<BV>().expect("BV")
    }

    fn as_i8_vec_unchecked(&self) -> &PVi8 {
        self.as_typed::<PVi8>().expect("PVi8")
    }

    fn as_i16_vec_unchecked(&self) -> &PVi16 {
        self.as_typed::<PVi16>().expect("PVi16")
    }

    fn as_i32_vec_unchecked(&self) -> &PVi32 {
        self.as_typed::<PVi32>().expect("PVi32")
    }

    fn as_i64_vec_unchecked(&self) -> &PVi64 {
        self.as_typed::<PVi64>().expect("PVi64")
    }

    fn as_u8_vec_unchecked(&self) -> &PVu8 {
        self.as_typed::<PVu8>().expect("PVu8")
    }

    fn as_u16_vec_unchecked(&self) -> &PVu16 {
        self.as_typed::<PVu16>().expect("PVu16")
    }

    fn as_u32_vec_unchecked(&self) -> &PVu32 {
        self.as_typed::<PVu32>().expect("PVu32")
    }

    fn as_u64_vec_unchecked(&self) -> &PVu64 {
        self.as_typed::<PVu64>().expect("PVu64")
    }

    fn as_f32_vec_unchecked(&self) -> &PVf32 {
        self.as_typed::<PVf32>().expect("PVf32")
    }

    fn as_f64_vec_unchecked(&self) -> &PVf64 {
        self.as_typed::<PVf64>().expect("PVf64")
    }

    fn as_str_vec_unchecked(&self) -> &UV {
        self.as_typed::<UV>().expect("UV")
    }
}
// ================================================================================================
// Default implementation for [FxSeq; W]
// ================================================================================================

impl<const W: usize, S> private::InnerEclectic for [S; W]
where
    S: FxSeq,
{
    type Seq = S;

    fn from_vec_seq(data: Vec<Self::Seq>) -> FxResult<Self>
    where
        Self: Sized,
    {
        if data.len() != W {
            return Err(FxError::LengthMismatch(data.len(), W));
        }

        Ok(data.try_into().unwrap())
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.as_slice()
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        for (i, arr) in arrays.into_iter().enumerate() {
            if i > W {
                break;
            }

            self[i] = arr;
        }

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        let mut res = Vec::new();
        for s in self.into_iter() {
            res.push(s);
        }

        res
    }
}

impl<const W: usize, S> private::InnerEclecticMutSeq for [S; W]
where
    S: FxSeq,
{
    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        self.as_mut_slice()
    }
}

impl<const W: usize, S> Confined for [S; W]
where
    S: FxSeq,
{
    fn width(&self) -> usize {
        W
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.iter().map(|s| s.data_type()).collect()
    }
}

impl<const W: usize, S> Confined for Vec<[S; W]>
where
    S: FxSeq,
{
    fn width(&self) -> usize {
        W
    }

    // if vector is empty then simply return empty datatype
    fn data_types(&self) -> Vec<&DataType> {
        if let Some(a) = self.first() {
            a.iter().map(|s| s.data_type()).collect()
        } else {
            vec![]
        }
    }
}

// ================================================================================================
// Default implementation for Vec<FxSeq>
// ================================================================================================

impl<S> private::InnerEclectic for Vec<S>
where
    S: FxSeq,
{
    type Seq = S;

    fn from_vec_seq(data: Vec<Self::Seq>) -> FxResult<Self>
    where
        Self: Sized,
    {
        Ok(data)
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.as_slice()
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        *self = arrays;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self
    }
}

impl<S> private::InnerEclecticMutSeq for Vec<S>
where
    S: FxSeq,
{
    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        self.as_mut_slice()
    }
}

impl<S> Confined for Vec<S>
where
    S: FxSeq,
{
    fn width(&self) -> usize {
        self.len()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.iter().map(|s| s.data_type()).collect()
    }
}

// ================================================================================================
// Default implementation for Chunk<dyn Array>
// ================================================================================================

impl private::InnerEclectic for ChunkArr {
    type Seq = ArcArr;

    fn from_vec_seq(data: Vec<Self::Seq>) -> FxResult<Self>
    where
        Self: Sized,
    {
        Ok(ChunkArr::try_new(data.to_vec())?)
    }
    fn ref_sequences(&self) -> &[Self::Seq] {
        self.arrays()
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        Chunk::try_new(arrays)?;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.into_arrays()
    }
}

impl private::InnerEclecticMutChunk for ChunkArr {
    fn mut_chunk(&mut self) -> &mut ChunkArr {
        self
    }
}

impl Confined for ChunkArr {
    fn width(&self) -> usize {
        self.arrays().len()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.arrays().iter().map(|a| a.data_type()).collect()
    }
}

impl Confined for Vec<ChunkArr> {
    // if the vector is empty then simply return 0
    fn width(&self) -> usize {
        if let Some(a) = self.first() {
            a.arrays().len()
        } else {
            0
        }
    }

    // if the vector is empty then simply return empty datatype
    fn data_types(&self) -> Vec<&DataType> {
        if let Some(a) = self.first() {
            a.arrays().iter().map(|a| a.data_type()).collect()
        } else {
            vec![]
        }
    }
}

// ================================================================================================
// Default implementation for Vec<E> where E: Eclectic
// ================================================================================================

impl<E: Eclectic + Confined> private::InnerReceptacle<false, usize, E> for Vec<E> {
    type OutRef<'a> = &'a E where Self: 'a;

    type OutMut<'a> = &'a mut E where Self: 'a;

    fn new_empty() -> Self {
        Vec::<E>::new()
    }

    fn ref_schema(&self) -> Option<&Schema> {
        None
    }

    fn get_chunk(&self, key: usize) -> FxResult<Self::OutRef<'_>> {
        self.get(key).ok_or(FxError::OutBounds)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<Self::OutMut<'_>> {
        self.get_mut(key).ok_or(FxError::OutBounds)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: E) -> FxResult<()> {
        if key > self.len() {
            return Err(FxError::OutBounds);
        }

        self.insert(key, data);

        Ok(())
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        if key > self.len() {
            return Err(FxError::OutBounds);
        }

        self.remove(key);

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: E) -> FxResult<()> {
        self.push(data);

        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.pop();

        Ok(())
    }
}

// ================================================================================================
// Default implementation for Map<I, Chunk<dyn Array>>
// ================================================================================================

impl<I, E> StaticPurport for HashMap<I, E>
where
    I: Hash + Eq,
    E: Eclectic,
{
}

impl<IDX, E> private::InnerReceptacle<false, IDX, E> for HashMap<IDX, E>
where
    IDX: Hash + Eq,
    E: Eclectic,
{
    type OutRef<'a> = &'a E where Self: 'a;

    type OutMut<'a> = &'a mut E where Self: 'a;

    fn new_empty() -> Self {
        HashMap::<IDX, E>::new()
    }

    fn ref_schema(&self) -> Option<&Schema> {
        None
    }

    fn get_chunk(&self, key: IDX) -> FxResult<Self::OutRef<'_>> {
        self.get(&key).ok_or(FxError::NoKey)
    }

    fn get_mut_chunk(&mut self, key: IDX) -> FxResult<Self::OutMut<'_>> {
        self.get_mut(&key).ok_or(FxError::NoKey)
    }

    fn insert_chunk_type_unchecked(&mut self, key: IDX, data: E) -> FxResult<()> {
        self.insert(key, data).ok_or(FxError::NoKey)?;

        Ok(())
    }

    fn remove_chunk(&mut self, key: IDX) -> FxResult<()> {
        self.remove(&key).ok_or(FxError::NoKey)?;

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, _data: E) -> FxResult<()> {
        unimplemented!()
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        unimplemented!()
    }
}
