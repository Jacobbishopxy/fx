//! file: value.rs
//! author: Jacob Xie
//! date: 2023/01/14 00:18:43 Saturday
//! brief: Value

use crate::macros::impl_from_x_for_value;

#[derive(Debug, PartialEq)]
pub enum FxValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(String),
    Null,
}

impl_from_x_for_value!(bool, Bool);
impl_from_x_for_value!(i8, I8);
impl_from_x_for_value!(i16, I16);
impl_from_x_for_value!(i32, I32);
impl_from_x_for_value!(i64, I64);
impl_from_x_for_value!(u8, U8);
impl_from_x_for_value!(u16, U16);
impl_from_x_for_value!(u32, U32);
impl_from_x_for_value!(u64, U64);
impl_from_x_for_value!(f32, F32);
impl_from_x_for_value!(f64, F64);
impl_from_x_for_value!(String, Str);

impl From<&str> for FxValue {
    fn from(value: &str) -> Self {
        FxValue::Str(value.to_owned())
    }
}

impl From<Option<&str>> for FxValue {
    fn from(value: Option<&str>) -> Self {
        match value {
            Some(v) => FxValue::Str(v.to_owned()),
            None => FxValue::Null,
        }
    }
}
