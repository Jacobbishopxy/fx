//! Value

use crate::FxError;

#[derive(Debug)]
pub enum FxValue {
    Null,
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
    OptU8(Option<u8>),
    OptU16(Option<u16>),
    OptU32(Option<u32>),
    OptU64(Option<u64>),
    OptI8(Option<i8>),
    OptI16(Option<i16>),
    OptI32(Option<i32>),
    OptI64(Option<i64>),
    OptF32(Option<f32>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptString(Option<String>),
}

macro_rules! fx_value_match {
    ($n:ident, $s:ident, Option<$t:ty>, $vr:ident) => {
        pub fn $n($s) -> $crate::FxResult<Option<$t>> {
            match $s {
                $crate::FxValue::$vr(v) => Ok(v),
                o => Err(FxError::InvalidCasting(format!("error type: {:?}", o))),
            }
        }
    };
    ($n:ident, $s:ident, $t:ty, $vr:ident) => {
        pub fn $n($s) -> $crate::FxResult<$t> {
            match $s {
                $crate::FxValue::$vr(v) => Ok(v),
                o => Err(FxError::InvalidCasting(format!("error type: {:?}", o))),
            }
        }
    };
}

impl FxValue {
    fx_value_match!(take_u8, self, u8, U8);
    fx_value_match!(take_u16, self, u16, U16);
    fx_value_match!(take_u32, self, u32, U32);
    fx_value_match!(take_u64, self, u64, U64);
    fx_value_match!(take_i8, self, i8, I8);
    fx_value_match!(take_i16, self, i16, I16);
    fx_value_match!(take_i32, self, i32, I32);
    fx_value_match!(take_i64, self, i64, I64);
    fx_value_match!(take_f32, self, f32, F32);
    fx_value_match!(take_f64, self, f64, F64);
    fx_value_match!(take_bool, self, bool, Bool);
    fx_value_match!(take_string, self, String, String);
    fx_value_match!(take_opt_u8, self, Option<u8>, OptU8);
    fx_value_match!(take_opt_u16, self, Option<u16>, OptU16);
    fx_value_match!(take_opt_u32, self, Option<u32>, OptU32);
    fx_value_match!(take_opt_u64, self, Option<u64>, OptU64);
    fx_value_match!(take_opt_i8, self, Option<i8>, OptI8);
    fx_value_match!(take_opt_i16, self, Option<i16>, OptI16);
    fx_value_match!(take_opt_i32, self, Option<i32>, OptI32);
    fx_value_match!(take_opt_i64, self, Option<i64>, OptI64);
    fx_value_match!(take_opt_f32, self, Option<f32>, OptF32);
    fx_value_match!(take_opt_f64, self, Option<f64>, OptF64);
    fx_value_match!(take_opt_bool, self, Option<bool>, OptBool);
    fx_value_match!(take_opt_string, self, Option<String>, OptString);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FxValueType {
    Null,
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    String,
    OptU8,
    OptU16,
    OptU32,
    OptU64,
    OptI8,
    OptI16,
    OptI32,
    OptI64,
    OptF32,
    OptF64,
    OptBool,
    OptString,
}

impl ToString for FxValueType {
    fn to_string(&self) -> String {
        format!("{:?}", &self)
    }
}

impl From<&FxValue> for FxValueType {
    fn from(v: &FxValue) -> Self {
        match v {
            FxValue::Null => FxValueType::Null,
            FxValue::U8(_) => FxValueType::U8,
            FxValue::U16(_) => FxValueType::U16,
            FxValue::U32(_) => FxValueType::U32,
            FxValue::U64(_) => FxValueType::U64,
            FxValue::I8(_) => FxValueType::I8,
            FxValue::I16(_) => FxValueType::I16,
            FxValue::I32(_) => FxValueType::I32,
            FxValue::I64(_) => FxValueType::I64,
            FxValue::F32(_) => FxValueType::F32,
            FxValue::F64(_) => FxValueType::F64,
            FxValue::Bool(_) => FxValueType::Bool,
            FxValue::String(_) => FxValueType::String,
            FxValue::OptU8(_) => FxValueType::OptU8,
            FxValue::OptU16(_) => FxValueType::OptU16,
            FxValue::OptU32(_) => FxValueType::OptU32,
            FxValue::OptU64(_) => FxValueType::OptU64,
            FxValue::OptI8(_) => FxValueType::OptI8,
            FxValue::OptI16(_) => FxValueType::OptI16,
            FxValue::OptI32(_) => FxValueType::OptI32,
            FxValue::OptI64(_) => FxValueType::OptI64,
            FxValue::OptF32(_) => FxValueType::OptF32,
            FxValue::OptF64(_) => FxValueType::OptF64,
            FxValue::OptBool(_) => FxValueType::OptBool,
            FxValue::OptString(_) => FxValueType::OptString,
        }
    }
}

#[test]
fn fx_value_get_success() {
    let a = FxValue::U16(233);

    println!("{:?}", a.take_u16());
}
