//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

// use arrow2::array::TryPush;
use arrow2::array::TryExtendFromSelf;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;

use super::{arr_to_vec, arr_to_vec_p, try_ext_from_slf};
use crate::types::*;
use crate::{FxError, FxResult};

// ================================================================================================
// Seq
// ================================================================================================

pub trait FxSeq {
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

    fn is_null(&self, idx: usize) -> Option<bool>;

    fn to_array(self) -> FxResult<ArcArr>;

    fn to_vector(self) -> FxResult<ArcVec>;

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self>;

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        for s in ss {
            Self::extend(self, s)?;
        }

        Ok(self)
    }
}

impl FxSeq for ArcArr {
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

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity().as_ref().map(|bm| bm.iter().collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }

    fn to_array(self) -> FxResult<ArcArr> {
        Ok(self)
    }

    fn to_vector(self) -> FxResult<ArcVec> {
        match &self.data_type() {
            DataType::Boolean => arr_to_vec!(self, BA, MB),
            DataType::Int8 => arr_to_vec_p!(self, PAi8, MPAi8),
            DataType::Int16 => arr_to_vec_p!(self, PAi16, MPAi16),
            DataType::Int32 => arr_to_vec_p!(self, PAi32, MPAi32),
            DataType::Int64 => arr_to_vec_p!(self, PAi64, MPAi64),
            DataType::UInt8 => arr_to_vec_p!(self, PAu8, MPAu8),
            DataType::UInt16 => arr_to_vec_p!(self, PAu16, MPAu16),
            DataType::UInt32 => arr_to_vec_p!(self, PAu32, MPAu32),
            DataType::UInt64 => arr_to_vec_p!(self, PAu64, MPAu64),
            DataType::Float32 => arr_to_vec_p!(self, PAf32, MPAf32),
            DataType::Float64 => arr_to_vec_p!(self, PAf64, MPAf64),
            DataType::Utf8 => arr_to_vec!(self, UA, MU),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn extend(&mut self, s: &ArcArr) -> FxResult<&mut Self> {
        let ct = concatenate(&[self.as_ref(), s.deref()])?;
        *self = Arc::from(ct);

        Ok(self)
    }
}

impl FxSeq for ArcVec {
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

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity().as_ref().map(|bm| bm.iter().collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }

    fn to_array(mut self) -> FxResult<ArcArr> {
        let res = Arc::get_mut(&mut self)
            .ok_or(FxError::FailedToConvert)?
            .as_arc();

        Ok(res)
    }

    fn to_vector(self) -> FxResult<ArcVec> {
        Ok(self)
    }

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self> {
        match &self.data_type() {
            DataType::Boolean => try_ext_from_slf!(self, s, MB),
            DataType::Int8 => try_ext_from_slf!(self, s, MPAi8),
            DataType::Int16 => try_ext_from_slf!(self, s, MPAi16),
            DataType::Int32 => try_ext_from_slf!(self, s, MPAi32),
            DataType::Int64 => try_ext_from_slf!(self, s, MPAi64),
            DataType::UInt8 => try_ext_from_slf!(self, s, MPAu8),
            DataType::UInt16 => try_ext_from_slf!(self, s, MPAu16),
            DataType::UInt32 => try_ext_from_slf!(self, s, MPAu32),
            DataType::UInt64 => try_ext_from_slf!(self, s, MPAu64),
            DataType::Float32 => try_ext_from_slf!(self, s, MPAf32),
            DataType::Float64 => try_ext_from_slf!(self, s, MPAf64),
            DataType::Utf8 => try_ext_from_slf!(self, s, MU),
            _ => Err(FxError::FailedToConvert),
        }
    }
}
