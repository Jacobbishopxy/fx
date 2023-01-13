//! file:	vector.rs
//! author: Jacob Xie
//! date:	2023/01/13 20:40:49 Friday
//! brief:	FxVector

use std::any::Any;

use arrow2::array::*;
use arrow2::datatypes::DataType;

use crate::{vec_impl_from_bool, vec_impl_from_native, vec_impl_from_str};
use crate::{FromSlice, FxError, FxResult};

// ================================================================================================
// FxVector
// ================================================================================================

#[derive(Debug)]
pub struct FxVector(Box<dyn MutableArray>);

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
            DataType::Boolean => {
                let val = (val as &dyn Any)
                    .downcast_ref::<bool>()
                    .ok_or_else(|| FxError::InvalidCasting("Invalid type".to_string()))?
                    .to_owned();

                self.0
                    .as_mut_any()
                    .downcast_mut::<MutableBooleanArray>()
                    .expect("expect MutableBooleanArray")
                    .try_push(Some(val))?;

                Ok(self)
            }
            DataType::Int8 => todo!(),
            DataType::Int16 => todo!(),
            DataType::Int32 => todo!(),
            DataType::Int64 => todo!(),
            DataType::UInt8 => todo!(),
            DataType::UInt16 => todo!(),
            DataType::UInt32 => todo!(),
            DataType::UInt64 => todo!(),
            DataType::Float16 => todo!(),
            DataType::Float32 => todo!(),
            DataType::Float64 => todo!(),
            DataType::Utf8 => todo!(),
            _ => Err(FxError::InvalidType("Unsupported type".to_string())),
        }
    }

    pub fn pop(&mut self) -> &mut Self {
        todo!()
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

impl FromSlice<String, FxVector> for FxVector {
    fn from_slice(slice: &[String]) -> Self {
        let iter = slice.into_iter().map(|e| e.as_str());
        FxVector(Box::new(
            arrow2::array::MutableUtf8Array::<i32>::from_iter_values(iter),
        ))
    }
}

impl FromSlice<&str, FxVector> for FxVector {
    fn from_slice(slice: &[&str]) -> Self {
        FxVector(Box::new(
            arrow2::array::MutableUtf8Array::<i32>::from_iter_values(slice.into_iter()),
        ))
    }
}

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
        let e = FxVector::from(vec![Some("x"), Some("y")]);
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
}
