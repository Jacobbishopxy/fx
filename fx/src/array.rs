//! Array

use std::collections::HashSet;

use arrow2::array::*;
use arrow2::datatypes::DataType;

use crate::{Datagrid, FxError, FxResult, FxValue};

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

    pub fn push<A>(&mut self, _v: FxValue) -> FxResult<&mut Self> {
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
    ($t:ty) => {
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
    };
}

impl_from_native!(u8);
impl_from_native!(u16);
impl_from_native!(u32);
impl_from_native!(u64);
impl_from_native!(i8);
impl_from_native!(i16);
impl_from_native!(i32);
impl_from_native!(i64);
impl_from_native!(f32);
impl_from_native!(f64);

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

// ================================================================================================
// Datagrid & FxArray conversions
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

impl From<Datagrid> for Vec<FxArray> {
    fn from(d: Datagrid) -> Self {
        d.into_arrays().into_iter().map(FxArray).collect()
    }
}

#[cfg(test)]
mod test_array {
    use super::*;

    #[test]
    fn from_vec_or_slice() {
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
}
