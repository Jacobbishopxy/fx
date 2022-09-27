//! Array

use std::collections::HashSet;

use arrow2::array::*;
use arrow2::datatypes::DataType;

use crate::{Datagrid, FxError, FxResult};

// ================================================================================================
// RawArray
// ================================================================================================

pub enum RawArray {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    I128(Vec<i128>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    String(Vec<String>),
    OptU8(Vec<Option<u8>>),
    OptU16(Vec<Option<u16>>),
    OptU32(Vec<Option<u32>>),
    OptU64(Vec<Option<u64>>),
    OptI8(Vec<Option<i8>>),
    OptI16(Vec<Option<i16>>),
    OptI32(Vec<Option<i32>>),
    OptI64(Vec<Option<i64>>),
    OptI128(Vec<Option<i128>>),
    OptF32(Vec<Option<f32>>),
    OptF64(Vec<Option<f64>>),
    OptBool(Vec<Option<bool>>),
    OptString(Vec<Option<String>>),
}

impl From<RawArray> for FxArray {
    fn from(ra: RawArray) -> Self {
        match ra {
            RawArray::U8(v) => FxArray::from(v),
            RawArray::U16(v) => FxArray::from(v),
            RawArray::U32(v) => FxArray::from(v),
            RawArray::U64(v) => FxArray::from(v),
            RawArray::I8(v) => FxArray::from(v),
            RawArray::I16(v) => FxArray::from(v),
            RawArray::I32(v) => FxArray::from(v),
            RawArray::I64(v) => FxArray::from(v),
            RawArray::I128(v) => FxArray::from(v),
            RawArray::F32(v) => FxArray::from(v),
            RawArray::F64(v) => FxArray::from(v),
            RawArray::Bool(v) => FxArray::from(v),
            RawArray::String(v) => FxArray::from(v),
            RawArray::OptU8(ov) => FxArray::from(ov),
            RawArray::OptU16(ov) => FxArray::from(ov),
            RawArray::OptU32(ov) => FxArray::from(ov),
            RawArray::OptU64(ov) => FxArray::from(ov),
            RawArray::OptI8(ov) => FxArray::from(ov),
            RawArray::OptI16(ov) => FxArray::from(ov),
            RawArray::OptI32(ov) => FxArray::from(ov),
            RawArray::OptI64(ov) => FxArray::from(ov),
            RawArray::OptI128(ov) => FxArray::from(ov),
            RawArray::OptF32(ov) => FxArray::from(ov),
            RawArray::OptF64(ov) => FxArray::from(ov),
            RawArray::OptBool(ov) => FxArray::from(ov),
            RawArray::OptString(ov) => FxArray::from(ov),
        }
    }
}

// ================================================================================================
// FxArray
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxArray(Box<dyn Array>);

impl FxArray {
    pub fn array(&self) -> &dyn Array {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.array().len()
    }

    pub fn is_empty(&self) -> bool {
        self.array().is_empty()
    }

    pub fn is_null(&self, i: usize) -> FxResult<bool> {
        if i >= self.len() {
            return Err(FxError::InvalidArgument(format!(
                "n: {i} is greater than array length: {}",
                self.len()
            )));
        }

        Ok(self.array().is_null(i))
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

    pub fn null_count(&self) -> usize {
        self.array().null_count()
    }

    pub fn has_null(&self) -> bool {
        self.null_count() > 0
    }

    pub fn push<A>(&mut self, _v: A) -> &mut Self {
        todo!()
    }

    pub fn pop(&mut self) -> &mut Self {
        todo!()
    }

    pub fn append(&mut self, _arr: &FxArray) -> &mut Self {
        todo!()
    }

    pub fn extend(&mut self, _arr: &FxArray) -> &mut Self {
        todo!()
    }
}

// ================================================================================================
// Constructors & Implements
// ================================================================================================

pub trait FromSlice<T> {
    fn from_slice(slice: &[T]) -> FxArray;
}

macro_rules! impl_from_native {
    ($t:ty, $vr:ident, $ovr:ident) => {
        impl From<Vec<$t>> for $crate::FxArray {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                FxArray(arrow2::array::PrimitiveArray::from(v).boxed())
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxArray {
            fn from(vec: Vec<Option<$t>>) -> Self {
                FxArray(arrow2::array::PrimitiveArray::from(vec).boxed())
            }
        }

        impl $crate::FromSlice<$t> for FxArray {
            fn from_slice(slice: &[$t]) -> Self {
                FxArray(arrow2::array::PrimitiveArray::from_slice(slice).boxed())
            }
        }

        impl From<Vec<$t>> for $crate::RawArray {
            fn from(vec: Vec<$t>) -> Self {
                $crate::RawArray::$vr(vec)
            }
        }

        impl From<Vec<Option<$t>>> for $crate::RawArray {
            fn from(vec: Vec<Option<$t>>) -> Self {
                $crate::RawArray::$ovr(vec)
            }
        }
    };
}

impl_from_native!(u8, U8, OptU8);
impl_from_native!(u16, U16, OptU16);
impl_from_native!(u32, U32, OptU32);
impl_from_native!(u64, U64, OptU64);
impl_from_native!(i8, I8, OptI8);
impl_from_native!(i16, I16, OptI16);
impl_from_native!(i32, I32, OptI32);
impl_from_native!(i64, I64, OptI64);
impl_from_native!(f32, F32, OptF32);
impl_from_native!(f64, F64, OptF64);
impl_from_native!(i128, I128, OptI128);

macro_rules! impl_from_str {
    ($t:ty) => {
        impl From<Vec<$t>> for $crate::FxArray {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                FxArray(arrow2::array::Utf8Array::<i32>::from(v).boxed())
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxArray {
            fn from(vec: Vec<Option<$t>>) -> Self {
                FxArray(arrow2::array::Utf8Array::<i32>::from(vec).boxed())
            }
        }

        impl $crate::FromSlice<$t> for FxArray {
            fn from_slice(slice: &[$t]) -> Self {
                FxArray(arrow2::array::Utf8Array::<i32>::from_slice(slice).boxed())
            }
        }
    };
}

impl_from_str!(&str);
impl_from_str!(String);

impl From<Vec<&str>> for RawArray {
    fn from(vec: Vec<&str>) -> Self {
        RawArray::String(vec.into_iter().map(String::from).collect())
    }
}

impl From<Vec<Option<&str>>> for RawArray {
    fn from(vec: Vec<Option<&str>>) -> Self {
        RawArray::OptString(
            vec.into_iter()
                .map(|ov| ov.map(String::from))
                .collect::<Vec<_>>(),
        )
    }
}

impl From<Vec<String>> for RawArray {
    fn from(vec: Vec<String>) -> Self {
        RawArray::String(vec)
    }
}

impl From<Vec<Option<String>>> for RawArray {
    fn from(vec: Vec<Option<String>>) -> Self {
        RawArray::OptString(vec)
    }
}

impl From<Vec<bool>> for FxArray {
    fn from(vec: Vec<bool>) -> Self {
        let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
        FxArray(BooleanArray::from(v).boxed())
    }
}

impl From<Vec<Option<bool>>> for FxArray {
    fn from(vec: Vec<Option<bool>>) -> Self {
        FxArray(BooleanArray::from(vec).boxed())
    }
}

impl FromSlice<bool> for FxArray {
    fn from_slice(slice: &[bool]) -> Self {
        FxArray(BooleanArray::from_slice(slice).boxed())
    }
}

impl From<Vec<bool>> for RawArray {
    fn from(vec: Vec<bool>) -> Self {
        RawArray::Bool(vec)
    }
}

impl From<Vec<Option<bool>>> for RawArray {
    fn from(vec: Vec<Option<bool>>) -> Self {
        RawArray::OptBool(vec)
    }
}

// ================================================================================================
// Datagrid & FxArray
// ================================================================================================

impl TryFrom<Vec<FxArray>> for Datagrid {
    type Error = FxError;

    fn try_from(value: Vec<FxArray>) -> Result<Self, Self::Error> {
        let iter = value.iter().map(|a| a.len());
        let lens = HashSet::<_>::from_iter(iter);
        if lens.len() != 1 {
            return Err(FxError::InvalidArgument(format!(
                "Vector of FxArray have different length: {:?}",
                lens
            )));
        }

        Ok(Datagrid::new(value.into_iter().map(|e| e.0).collect()))
    }
}

#[test]
fn test_from_v() {
    let a = FxArray::from(vec![1u8, 23]);

    let b = FxArray::from(vec![Some(1), Some(2)]);
    let c = FxArray::from_slice(&[1, 2]);

    let d = FxArray::from_slice(&["a", "c"]);
    let e = FxArray::from(vec![Some("x"), Some("y")]);
    let f = FxArray::from_slice(&[true, false]);

    println!("{:?}", a);
    println!("{:?}", b);
    println!("{:?}", c);
    println!("{:?}", d);
    println!("{:?}", e);
    println!("{:?}", f);
}
