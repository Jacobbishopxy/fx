//! Value

#[derive(Debug)]
pub enum FxValue {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FxValueType {
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

impl FxValueType {
    pub fn gen_u8_vec(&self) -> Vec<u8> {
        vec![]
    }

    pub fn gen_u16_vec(&self) -> Vec<u16> {
        vec![]
    }

    pub fn gen_u32_vec(&self) -> Vec<u32> {
        vec![]
    }

    pub fn gen_u64_vec(&self) -> Vec<u64> {
        vec![]
    }

    pub fn gen_i8_vec(&self) -> Vec<i8> {
        vec![]
    }

    pub fn gen_i16_vec(&self) -> Vec<i16> {
        vec![]
    }

    pub fn gen_i32_vec(&self) -> Vec<i32> {
        vec![]
    }

    pub fn gen_i64_vec(&self) -> Vec<i64> {
        vec![]
    }

    pub fn gen_f32_vec(&self) -> Vec<f32> {
        vec![]
    }

    pub fn gen_f64_vec(&self) -> Vec<f64> {
        vec![]
    }

    pub fn gen_bool_vec(&self) -> Vec<bool> {
        vec![]
    }

    pub fn gen_string_vec(&self) -> Vec<String> {
        vec![]
    }
}

impl From<&FxValue> for FxValueType {
    fn from(v: &FxValue) -> Self {
        match v {
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
