//! Array

use std::sync::Arc;

use arrow2::array::*;
use arrow2::bitmap::Bitmap;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;
use ref_cast::RefCast;

use crate::macros::*;
use crate::{FromSlice, FxResult};

// ================================================================================================
// FxArray
// ================================================================================================

#[derive(Debug, Clone, RefCast)]
#[repr(transparent)]
pub struct FxArray(pub(crate) Arc<dyn Array>);

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
        invalid_arg!(self, i);

        Ok(self.array().is_null(i))
    }

    pub fn is_valid(&self, i: usize) -> FxResult<bool> {
        invalid_arg!(self, i);

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

    pub fn iter() {
        todo!()
    }

    pub fn slice() {
        todo!()
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.0.validity()
    }

    pub fn set_validity() {
        todo!()
    }

    pub fn value() {
        todo!()
    }

    pub fn set_value() {
        todo!()
    }

    pub fn values() {
        todo!()
    }

    pub fn set_values() {
        todo!()
    }

    pub fn values_iter() {
        todo!()
    }

    pub fn extend(&mut self, array: &FxArray) -> FxResult<&mut Self> {
        self.0 = Arc::from(concatenate(&[self.array(), array.array()])?);

        Ok(self)
    }
}

// ================================================================================================
// Constructors & Implements
// ================================================================================================

arr_impl_from_native!(u8);
arr_impl_from_native!(u16);
arr_impl_from_native!(u32);
arr_impl_from_native!(u64);
arr_impl_from_native!(i8);
arr_impl_from_native!(i16);
arr_impl_from_native!(i32);
arr_impl_from_native!(i64);
arr_impl_from_native!(f32);
arr_impl_from_native!(f64);

arr_impl_from_str!(&str);
arr_impl_from_str!(String);

arr_impl_from_bool!();

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

    #[test]
    fn extent_should_be_successful() {
        let mut a = FxArray::from(vec![None, Some("x")]);
        let b = FxArray::from(vec![None, Some("y"), None]);

        let res = a.extend(&b);
        assert!(res.is_ok());

        println!("{a:?}");
    }
}
