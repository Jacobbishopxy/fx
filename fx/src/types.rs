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

pub enum ArrEnum {
    BA(BA),
    PAi8(PAi8),
    PAi16(PAi16),
    PAi32(PAi32),
    PAi64(PAi64),
    PAu8(PAu8),
    PAu16(PAu16),
    PAu32(PAu32),
    PAu64(PAu64),
    PAf32(PAf32),
    PAf64(PAf64),
    UA(UA),
}

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

pub enum VecEnum {
    BV(BV),
    PVi8(PVi8),
    PVi16(PVi16),
    PVi32(PVi32),
    PVi64(PVi64),
    PVu8(PVu8),
    PVu16(PVu16),
    PVu32(PVu32),
    PVu64(PVu64),
    PVf32(PVf32),
    PVf64(PVf64),
    UV(UV),
}
