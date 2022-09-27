//! Array

use arrow2::array::*;

use crate::Datagrid;

#[derive(Debug, Clone)]
pub struct FxArray(Box<dyn Array>);

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
impl_from_native!(i128);

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

impl From<Vec<FxArray>> for Datagrid {
    fn from(v: Vec<FxArray>) -> Self {
        Datagrid::new(v.into_iter().map(|e| e.0).collect())
    }
}

impl From<&[FxArray]> for Datagrid {
    fn from(s: &[FxArray]) -> Self {
        Datagrid::new(s.iter().map(|e| e.0.clone()).collect())
    }
}

#[test]
fn test_from_v() {
    let a = FxArray::from(vec![1u8, 23]);

    let b = FxArray::from(vec![Some(1), Some(2)]);
    let c = FxArray::from_slice(&[1, 2]);

    let d = FxArray::from_slice(&["a", "c"]);
    let e = FxArray::from(vec![Some("x"), Some("y")]);

    println!("{:?}", a);
    println!("{:?}", b);
    println!("{:?}", c);
    println!("{:?}", d);
    println!("{:?}", e);
}
