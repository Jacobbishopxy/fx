//! file:	value.rs
//! author:	Jacob Xie
//! date:	2023/01/14 00:18:43 Saturday
//! brief:	Value

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
}
