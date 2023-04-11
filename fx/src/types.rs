//! file: types.rs
//! author: Jacob Xie
//! date: 2023/01/14 19:12:30 Saturday
//! brief: Types

use arrow2::array::*;

pub(crate) type BA = BooleanArray;
pub(crate) type PAi8 = PrimitiveArray<i8>;
pub(crate) type PAi16 = PrimitiveArray<i16>;
pub(crate) type PAi32 = PrimitiveArray<i32>;
pub(crate) type PAi64 = PrimitiveArray<i64>;
pub(crate) type PAu8 = PrimitiveArray<u8>;
pub(crate) type PAu16 = PrimitiveArray<u16>;
pub(crate) type PAu32 = PrimitiveArray<u32>;
pub(crate) type PAu64 = PrimitiveArray<u64>;
pub(crate) type PAf32 = PrimitiveArray<f32>;
pub(crate) type PAf64 = PrimitiveArray<f64>;
pub(crate) type UA = Utf8Array<i32>;
pub(crate) type NA = NullArray;

#[allow(dead_code)]
pub(crate) enum ArrEnum {
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

pub(crate) type BV = MutableBooleanArray;
pub(crate) type PVi8 = MutablePrimitiveArray<i8>;
pub(crate) type PVi16 = MutablePrimitiveArray<i16>;
pub(crate) type PVi32 = MutablePrimitiveArray<i32>;
pub(crate) type PVi64 = MutablePrimitiveArray<i64>;
pub(crate) type PVu8 = MutablePrimitiveArray<u8>;
pub(crate) type PVu16 = MutablePrimitiveArray<u16>;
pub(crate) type PVu32 = MutablePrimitiveArray<u32>;
pub(crate) type PVu64 = MutablePrimitiveArray<u64>;
pub(crate) type PVf32 = MutablePrimitiveArray<f32>;
pub(crate) type PVf64 = MutablePrimitiveArray<f64>;
pub(crate) type UV = MutableUtf8Array<i32>;

#[allow(dead_code)]
pub(crate) enum VecEnum {
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
