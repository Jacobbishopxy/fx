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

pub(crate) type MB = MutableBooleanArray;
pub(crate) type MPAi8 = MutablePrimitiveArray<i8>;
pub(crate) type MPAi16 = MutablePrimitiveArray<i16>;
pub(crate) type MPAi32 = MutablePrimitiveArray<i32>;
pub(crate) type MPAi64 = MutablePrimitiveArray<i64>;
pub(crate) type MPAu8 = MutablePrimitiveArray<u8>;
pub(crate) type MPAu16 = MutablePrimitiveArray<u16>;
pub(crate) type MPAu32 = MutablePrimitiveArray<u32>;
pub(crate) type MPAu64 = MutablePrimitiveArray<u64>;
pub(crate) type MPAf32 = MutablePrimitiveArray<f32>;
pub(crate) type MPAf64 = MutablePrimitiveArray<f64>;
pub(crate) type MU = MutableUtf8Array<i32>;
