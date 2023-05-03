//! file: types.rs
//! author: Jacob Xie
//! date: 2023/01/14 19:12:30 Saturday
//! brief: Types

use arrow2::array::*;

pub type BA = BooleanArray;
pub type PAi8 = PrimitiveArray<i8>;
pub type PAi16 = PrimitiveArray<i16>;
pub type PAi32 = PrimitiveArray<i32>;
pub type PAi64 = PrimitiveArray<i64>;
pub type PAu8 = PrimitiveArray<u8>;
pub type PAu16 = PrimitiveArray<u16>;
pub type PAu32 = PrimitiveArray<u32>;
pub type PAu64 = PrimitiveArray<u64>;
pub type PAf32 = PrimitiveArray<f32>;
pub type PAf64 = PrimitiveArray<f64>;
pub type UA = Utf8Array<i32>;
pub type NA = NullArray;

pub enum ArrEnum<'a> {
    BA(&'a BA),
    PAi8(&'a PAi8),
    PAi16(&'a PAi16),
    PAi32(&'a PAi32),
    PAi64(&'a PAi64),
    PAu8(&'a PAu8),
    PAu16(&'a PAu16),
    PAu32(&'a PAu32),
    PAu64(&'a PAu64),
    PAf32(&'a PAf32),
    PAf64(&'a PAf64),
    UA(&'a UA),
}

macro_rules! arr_enum_from_x {
    ($t:ident) => {
        impl<'a> From<&'a $t> for ArrEnum<'a> {
            fn from(value: &'a $t) -> Self {
                ArrEnum::$t(value)
            }
        }
    };
}

arr_enum_from_x!(BA);
arr_enum_from_x!(PAi8);
arr_enum_from_x!(PAi16);
arr_enum_from_x!(PAi32);
arr_enum_from_x!(PAi64);
arr_enum_from_x!(PAu8);
arr_enum_from_x!(PAu16);
arr_enum_from_x!(PAu32);
arr_enum_from_x!(PAu64);
arr_enum_from_x!(PAf32);
arr_enum_from_x!(PAf64);
arr_enum_from_x!(UA);

pub type BV = MutableBooleanArray;
pub type PVi8 = MutablePrimitiveArray<i8>;
pub type PVi16 = MutablePrimitiveArray<i16>;
pub type PVi32 = MutablePrimitiveArray<i32>;
pub type PVi64 = MutablePrimitiveArray<i64>;
pub type PVu8 = MutablePrimitiveArray<u8>;
pub type PVu16 = MutablePrimitiveArray<u16>;
pub type PVu32 = MutablePrimitiveArray<u32>;
pub type PVu64 = MutablePrimitiveArray<u64>;
pub type PVf32 = MutablePrimitiveArray<f32>;
pub type PVf64 = MutablePrimitiveArray<f64>;
pub type UV = MutableUtf8Array<i32>;

pub enum VecEnum<'a> {
    BV(&'a BV),
    PVi8(&'a PVi8),
    PVi16(&'a PVi16),
    PVi32(&'a PVi32),
    PVi64(&'a PVi64),
    PVu8(&'a PVu8),
    PVu16(&'a PVu16),
    PVu32(&'a PVu32),
    PVu64(&'a PVu64),
    PVf32(&'a PVf32),
    PVf64(&'a PVf64),
    UV(&'a UV),
}

macro_rules! vec_enum_from_x {
    ($t:ident) => {
        impl<'a> From<&'a $t> for VecEnum<'a> {
            fn from(value: &'a $t) -> Self {
                VecEnum::$t(value)
            }
        }
    };
}

vec_enum_from_x!(BV);
vec_enum_from_x!(PVi8);
vec_enum_from_x!(PVi16);
vec_enum_from_x!(PVi32);
vec_enum_from_x!(PVi64);
vec_enum_from_x!(PVu8);
vec_enum_from_x!(PVu16);
vec_enum_from_x!(PVu32);
vec_enum_from_x!(PVu64);
vec_enum_from_x!(PVf32);
vec_enum_from_x!(PVf64);
vec_enum_from_x!(UV);
