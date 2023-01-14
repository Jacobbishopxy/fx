//! file:	vector.rs
//! author: Jacob Xie
//! date:	2023/01/13 20:40:49 Friday
//! brief:	FxVector

use std::any::Any;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::DataType;
use ref_cast::RefCast;

use crate::macros::*;
use crate::types::*;
use crate::{FromSlice, FxError, FxResult, FxValue};

// ================================================================================================
// FxVector
// ================================================================================================

#[derive(Debug, Clone, RefCast)]
#[repr(transparent)]
pub struct FxVector(pub(crate) Arc<dyn MutableArray>);

impl FxVector {
    pub fn array(&self) -> &dyn MutableArray {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.array().len()
    }

    pub fn is_empty(&self) -> bool {
        self.array().is_empty()
    }

    pub fn is_valid(&self, i: usize) -> FxResult<bool> {
        if i >= self.len() {
            return Err(FxError::InvalidArgument(format!(
                "n: {i} is greater than array length: {}",
                self.len()
            )));
        }

        Ok(self.array().is_valid(i))
    }

    pub fn data_type(&self) -> &DataType {
        self.array().data_type()
    }

    pub fn push<A: Any>(&mut self, val: &A) -> FxResult<&mut Self> {
        match self.data_type() {
            DataType::Boolean => vec_push_branch!(self, val, bool, MBA),
            DataType::Int8 => vec_push_branch!(self, val, i8, MPAi8),
            DataType::Int16 => vec_push_branch!(self, val, i16, MPAi16),
            DataType::Int32 => vec_push_branch!(self, val, i32, MPAi32),
            DataType::Int64 => vec_push_branch!(self, val, i64, MPAi64),
            DataType::UInt8 => vec_push_branch!(self, val, u8, MPAu8),
            DataType::UInt16 => vec_push_branch!(self, val, u16, MPAu16),
            DataType::UInt32 => vec_push_branch!(self, val, u32, MPAu32),
            DataType::UInt64 => vec_push_branch!(self, val, u64, MPAu64),
            DataType::Float32 => vec_push_branch!(self, val, f32, MPAf32),
            DataType::Float64 => vec_push_branch!(self, val, f64, MPAf64),
            DataType::Utf8 => vec_push_branch!(self, val, String, MUA),
            _ => Err(FxError::InvalidType("Unsupported type".to_string())),
        }
    }

    pub fn pop(&mut self) -> FxResult<&mut Self> {
        match self.data_type() {
            DataType::Boolean => vec_pop_branch!(self, MBA),
            DataType::Int8 => vec_pop_branch!(self, MPAi8),
            DataType::Int16 => vec_pop_branch!(self, MPAi16),
            DataType::Int32 => vec_pop_branch!(self, MPAi32),
            DataType::Int64 => vec_pop_branch!(self, MPAi64),
            DataType::UInt8 => vec_pop_branch!(self, MPAu8),
            DataType::UInt16 => vec_pop_branch!(self, MPAu16),
            DataType::UInt32 => vec_pop_branch!(self, MPAu32),
            DataType::UInt64 => vec_pop_branch!(self, MPAu64),
            DataType::Float32 => vec_pop_branch!(self, MPAf32),
            DataType::Float64 => vec_pop_branch!(self, MPAf64),
            DataType::Utf8 => vec_pop_branch!(self, MUA),
            _ => Err(FxError::InvalidType("Unsupported type".to_string())),
        }
    }

    pub fn pop_val(&mut self) -> Option<FxValue> {
        match self.data_type() {
            DataType::Boolean => vec_pop_branch!(self, MBA, Bool),
            DataType::Int8 => vec_pop_branch!(self, MPAi8, I8),
            DataType::Int16 => vec_pop_branch!(self, MPAi16, I16),
            DataType::Int32 => vec_pop_branch!(self, MPAi32, I32),
            DataType::Int64 => vec_pop_branch!(self, MPAi64, I64),
            DataType::UInt8 => vec_pop_branch!(self, MPAu8, U8),
            DataType::UInt16 => vec_pop_branch!(self, MPAu16, U16),
            DataType::UInt32 => vec_pop_branch!(self, MPAu32, U32),
            DataType::UInt64 => vec_pop_branch!(self, MPAu64, U64),
            DataType::Float32 => vec_pop_branch!(self, MPAf32, F32),
            DataType::Float64 => vec_pop_branch!(self, MPAf64, F64),
            DataType::Utf8 => vec_pop_branch!(self, MUA, Str),
            _ => None,
        }
    }

    pub fn append(&mut self, _arr: &FxVector) -> &mut Self {
        todo!()
    }

    pub fn extend(&mut self, _arr: &FxVector) -> &mut Self {
        todo!()
    }
}

// ================================================================================================
// Constructors & Implements
// ================================================================================================

vec_impl_from_native!(u8);
vec_impl_from_native!(u16);
vec_impl_from_native!(u32);
vec_impl_from_native!(u64);
vec_impl_from_native!(i8);
vec_impl_from_native!(i16);
vec_impl_from_native!(i32);
vec_impl_from_native!(i64);
vec_impl_from_native!(i128);
vec_impl_from_native!(f32);
vec_impl_from_native!(f64);

vec_impl_from_str!(&str);
vec_impl_from_str!(String);

vec_impl_from_bool!();

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_vector {
    use super::*;

    #[test]
    fn from_vec_or_slice() {
        let a = FxVector::from(vec![1u8, 23]);

        let b = FxVector::from(vec![Some(1), Some(2)]);
        let c = FxVector::from_slice(&[1, 2]);

        let d = FxVector::from_slice(&["a", "c"]);
        let e = FxVector::from(vec![Some("x"), Some("y"), None]);
        let f = FxVector::from_slice(&[true, false]);

        println!("{a:?}");
        println!("{b:?}");
        println!("{c:?}");
        println!("{d:?}");
        println!("{e:?}");
        println!("{f:?}");
    }

    #[test]
    fn push_value_should_be_success() {
        let mut fx_vec = FxVector::from_slice(&[true, false, true, true]);
        let res = fx_vec.push(&true);

        assert!(res.is_ok());

        println!("{fx_vec:?}");
    }

    #[test]
    fn pop_value_should_be_success() {
        let mut fx_vec = FxVector::from_slice(&[true, false, true, true]);
        let res1 = fx_vec.pop();
        assert!(res1.is_ok());

        let res2 = fx_vec.pop_val();
        assert_eq!(res2.unwrap(), FxValue::Bool(true));

        println!("{fx_vec:?}");
    }
}
