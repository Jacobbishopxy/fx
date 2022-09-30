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
}

macro_rules! fx_value_match {
    ($n:ident, $s:ident, $t:ident, $vr:ident) => {
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
        }
    }
}

#[test]
fn fx_value_get_success() {
    let a = FxValue::U16(233);

    println!("{:?}", a.take_u16());
}
