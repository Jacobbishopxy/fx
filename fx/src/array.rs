//! Array

use arrow2::array::*;
use arrow2::datatypes::DataType;

use crate::{FromSlice, FxError, FxResult};

// ================================================================================================
// FxArray
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxArray(pub(crate) Box<dyn Array>);

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
}

// ================================================================================================
// Constructors & Implements
// ================================================================================================

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

        impl $crate::FromSlice<$t, FxArray> for FxArray {
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

        impl $crate::FromSlice<$t, FxArray> for FxArray {
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
        FxArray(arrow2::array::BooleanArray::from(v).boxed())
    }
}

impl From<Vec<Option<bool>>> for FxArray {
    fn from(vec: Vec<Option<bool>>) -> Self {
        FxArray(arrow2::array::BooleanArray::from(vec).boxed())
    }
}

impl FromSlice<bool, FxArray> for FxArray {
    fn from_slice(slice: &[bool]) -> Self {
        FxArray(arrow2::array::BooleanArray::from_slice(slice).boxed())
    }
}

// ================================================================================================
// Test
// ================================================================================================

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

        println!("{a:?}");
        println!("{b:?}");
        println!("{c:?}");
        println!("{d:?}");
        println!("{e:?}");
        println!("{f:?}");
    }
}
