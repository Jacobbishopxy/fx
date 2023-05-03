//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;

use arrow2::array::Array;
use arrow2::datatypes::DataType;

use crate::cont::{ArcArr, ArcVec, BoxArr, BoxVec};
use crate::error::{FxError, FxResult};
use crate::types::*;
use crate::value::FxValue;

// ================================================================================================
// Seq
// ================================================================================================

pub trait FxSeq: Debug {
    fn from_ref(data: &dyn Array) -> Self;

    fn from_box_arr(data: Box<dyn Array>) -> Self;

    fn new_nulls(datatype: DataType, length: usize) -> Self;

    fn new_empty(datatype: DataType) -> Self;

    fn is_arr() -> bool;

    fn is_vec() -> bool;

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> Option<&mut dyn Any>;

    // default impl
    fn as_typed<T: 'static>(&self) -> FxResult<&T> {
        self.as_any()
            .downcast_ref::<T>()
            .ok_or(FxError::InvalidDowncast)
    }

    // default impl
    fn as_typed_mut<T: 'static>(&mut self) -> FxResult<&mut T> {
        self.as_any_mut()
            .ok_or(FxError::InvalidDowncast)?
            .downcast_mut::<T>()
            .ok_or(FxError::InvalidDowncast)
    }

    fn as_arr_enum(&self) -> FxResult<ArrEnum> {
        if Self::is_vec() {
            Err(FxError::InvalidType("Vec".to_owned()))
        } else {
            match self.data_type() {
                DataType::Boolean => Ok(ArrEnum::from(self.as_typed::<BA>().unwrap())),
                DataType::Int8 => Ok(ArrEnum::from(self.as_typed::<PAi8>().unwrap())),
                DataType::Int16 => Ok(ArrEnum::from(self.as_typed::<PAi16>().unwrap())),
                DataType::Int32 => Ok(ArrEnum::from(self.as_typed::<PAi32>().unwrap())),
                DataType::Int64 => Ok(ArrEnum::from(self.as_typed::<PAi64>().unwrap())),
                DataType::UInt8 => Ok(ArrEnum::from(self.as_typed::<PAu8>().unwrap())),
                DataType::UInt16 => Ok(ArrEnum::from(self.as_typed::<PAu16>().unwrap())),
                DataType::UInt32 => Ok(ArrEnum::from(self.as_typed::<PAu32>().unwrap())),
                DataType::UInt64 => Ok(ArrEnum::from(self.as_typed::<PAu64>().unwrap())),
                DataType::Float32 => Ok(ArrEnum::from(self.as_typed::<PAf32>().unwrap())),
                DataType::Float64 => Ok(ArrEnum::from(self.as_typed::<PAf64>().unwrap())),
                DataType::Utf8 => Ok(ArrEnum::from(self.as_typed::<UA>().unwrap())),
                o => Err(FxError::InvalidType(format!("{:?}", o))),
            }
        }
    }

    fn as_vec_enum(&self) -> FxResult<VecEnum> {
        if Self::is_arr() {
            Err(FxError::InvalidType("Arr".to_owned()))
        } else {
            match self.data_type() {
                DataType::Boolean => Ok(VecEnum::from(self.as_typed::<BV>().unwrap())),
                DataType::Int8 => Ok(VecEnum::from(self.as_typed::<PVi8>().unwrap())),
                DataType::Int16 => Ok(VecEnum::from(self.as_typed::<PVi16>().unwrap())),
                DataType::Int32 => Ok(VecEnum::from(self.as_typed::<PVi32>().unwrap())),
                DataType::Int64 => Ok(VecEnum::from(self.as_typed::<PVi64>().unwrap())),
                DataType::UInt8 => Ok(VecEnum::from(self.as_typed::<PVu8>().unwrap())),
                DataType::UInt16 => Ok(VecEnum::from(self.as_typed::<PVu16>().unwrap())),
                DataType::UInt32 => Ok(VecEnum::from(self.as_typed::<PVu32>().unwrap())),
                DataType::UInt64 => Ok(VecEnum::from(self.as_typed::<PVu64>().unwrap())),
                DataType::Float32 => Ok(VecEnum::from(self.as_typed::<PVf32>().unwrap())),
                DataType::Float64 => Ok(VecEnum::from(self.as_typed::<PVf64>().unwrap())),
                DataType::Utf8 => Ok(VecEnum::from(self.as_typed::<UV>().unwrap())),
                o => Err(FxError::InvalidType(format!("{:?}", o))),
            }
        }
    }

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn data_type(&self) -> &DataType;

    fn get_validity(&self) -> Option<Vec<bool>>;

    fn has_null(&self) -> bool {
        self.get_validity().is_some()
    }

    fn is_valid(&self, idx: usize) -> Option<bool>;

    fn to_arc_array(self) -> FxResult<ArcArr>;

    fn to_box_array(self) -> FxResult<BoxArr>;

    fn to_arc_vector(self) -> FxResult<ArcVec>;

    fn to_box_vector(self) -> FxResult<BoxVec>;

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self>;

    fn concat(&mut self, ss: &[&Self]) -> FxResult<&mut Self> {
        for s in ss {
            Self::extend(self, s)?;
        }

        Ok(self)
    }
}

// ================================================================================================
// AsArray
// ================================================================================================

pub trait AsArray {
    fn as_bool_arr_unchecked(&self) -> &BA;
    fn as_i8_arr_unchecked(&self) -> &PAi8;
    fn as_i16_arr_unchecked(&self) -> &PAi16;
    fn as_i32_arr_unchecked(&self) -> &PAi32;
    fn as_i64_arr_unchecked(&self) -> &PAi64;
    fn as_u8_arr_unchecked(&self) -> &PAu8;
    fn as_u16_arr_unchecked(&self) -> &PAu16;
    fn as_u32_arr_unchecked(&self) -> &PAu32;
    fn as_u64_arr_unchecked(&self) -> &PAu64;
    fn as_f32_arr_unchecked(&self) -> &PAf32;
    fn as_f64_arr_unchecked(&self) -> &PAf64;
    fn as_str_arr_unchecked(&self) -> &UA;
}

// ================================================================================================
// AsVector (MutableArray)
// ================================================================================================

pub trait AsVector {
    fn as_bool_vec_unchecked(&self) -> &BV;
    fn as_i8_vec_unchecked(&self) -> &PVi8;
    fn as_i16_vec_unchecked(&self) -> &PVi16;
    fn as_i32_vec_unchecked(&self) -> &PVi32;
    fn as_i64_vec_unchecked(&self) -> &PVi64;
    fn as_u8_vec_unchecked(&self) -> &PVu8;
    fn as_u16_vec_unchecked(&self) -> &PVu16;
    fn as_u32_vec_unchecked(&self) -> &PVu32;
    fn as_u64_vec_unchecked(&self) -> &PVu64;
    fn as_f32_vec_unchecked(&self) -> &PVf32;
    fn as_f64_vec_unchecked(&self) -> &PVf64;
    fn as_str_vec_unchecked(&self) -> &UV;
}

// ================================================================================================
// IntoIterator (Arr)
// ================================================================================================

macro_rules! next_arr_val {
    ($s:expr, $ar:ident) => {
        if let ArrEnum::$ar(a) = $s.data {
            Some(FxValue::from(a.get($s.index).clone()))
        } else {
            None
        }
    };
}

pub struct FxArrIntoIterator<'a, T: FxSeq> {
    data_type: &'a DataType,
    data: ArrEnum<'a>,
    index: usize,
    len: usize,
    _p: PhantomData<T>,
}

impl<'a, T: FxSeq> FxArrIntoIterator<'a, T> {
    pub fn new(s: &'a T) -> FxResult<Self> {
        if T::is_vec() {
            Err(FxError::InvalidType("Vec".to_owned()))
        } else {
            Ok(FxArrIntoIterator {
                data_type: s.data_type(),
                data: s.as_arr_enum()?,
                index: 0,
                len: s.len(),
                _p: PhantomData,
            })
        }
    }
}

impl<'a, T: FxSeq> Iterator for FxArrIntoIterator<'a, T> {
    type Item = FxValue;

    fn next(&mut self) -> Option<Self::Item> {
        // dbg!(self.index);
        // dbg!(self.len);
        if self.index >= self.len {
            return None;
        }
        let val = match self.data_type {
            DataType::Boolean => next_arr_val!(self, BA),
            DataType::Int8 => next_arr_val!(self, PAi8),
            DataType::Int16 => next_arr_val!(self, PAi16),
            DataType::Int32 => next_arr_val!(self, PAi32),
            DataType::Int64 => next_arr_val!(self, PAi64),
            DataType::UInt8 => next_arr_val!(self, PAu8),
            DataType::UInt16 => next_arr_val!(self, PAu16),
            DataType::UInt32 => next_arr_val!(self, PAu32),
            DataType::UInt64 => next_arr_val!(self, PAu64),
            DataType::Float32 => next_arr_val!(self, PAf32),
            DataType::Float64 => next_arr_val!(self, PAf64),
            DataType::Utf8 => next_arr_val!(self, UA),
            _ => unimplemented!(),
        };
        if val.is_some() {
            self.index += 1;
        }
        val
    }
}

pub struct FxArcArr<'a>(&'a ArcArr);

pub struct FxBoxArr<'a>(&'a BoxArr);

impl<'a> IntoIterator for FxArcArr<'a> {
    type Item = FxValue;
    type IntoIter = FxArrIntoIterator<'a, ArcArr>;

    fn into_iter(self) -> Self::IntoIter {
        FxArrIntoIterator::<ArcArr>::new(self.0).unwrap()
    }
}
impl<'a> IntoIterator for FxBoxArr<'a> {
    type Item = FxValue;
    type IntoIter = FxArrIntoIterator<'a, BoxArr>;

    fn into_iter(self) -> Self::IntoIter {
        FxArrIntoIterator::<BoxArr>::new(self.0).unwrap()
    }
}

// ================================================================================================
// IntoIterator (Vec)
// ================================================================================================

macro_rules! next_vec_val {
    ($s:expr, $ar:ident) => {
        if let VecEnum::$ar(v) = $s.data {
            v.values().get($s.index).cloned().map(FxValue::from)
            // let c = v.values().get($s.index).cloned();
            // dbg!(&c);
            // c.map(FxValue::from)
        } else {
            None
        }
    };
}

pub struct FxVecIntoIterator<'a, T: FxSeq> {
    data_type: &'a DataType,
    data: VecEnum<'a>,
    valid: Option<Vec<bool>>,
    index: usize,
    len: usize,
    _p: PhantomData<T>,
}

impl<'a, T: FxSeq> FxVecIntoIterator<'a, T> {
    pub fn new(s: &'a T) -> FxResult<Self> {
        if T::is_arr() {
            Err(FxError::InvalidType("Arr".to_owned()))
        } else {
            Ok(FxVecIntoIterator {
                data_type: s.data_type(),
                data: s.as_vec_enum()?,
                valid: s.get_validity(),
                index: 0,
                len: s.len(),
                _p: PhantomData,
            })
        }
    }
}

impl<'a, T: FxSeq> Iterator for FxVecIntoIterator<'a, T> {
    type Item = FxValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        if let Some(ref ns) = self.valid {
            if !*ns.get(self.index).unwrap() {
                self.index += 1;
                return Some(FxValue::Null);
            }
        }

        let val = match self.data_type {
            DataType::Boolean => {
                if let VecEnum::BV(v) = self.data {
                    Some(FxValue::from(v.values().get(self.index)))
                } else {
                    None
                }
            }
            DataType::Int8 => next_vec_val!(self, PVi8),
            DataType::Int16 => next_vec_val!(self, PVi16),
            DataType::Int32 => next_vec_val!(self, PVi32),
            DataType::Int64 => next_vec_val!(self, PVi64),
            DataType::UInt8 => next_vec_val!(self, PVu8),
            DataType::UInt16 => next_vec_val!(self, PVu16),
            DataType::UInt32 => next_vec_val!(self, PVu32),
            DataType::UInt64 => next_vec_val!(self, PVu64),
            DataType::Float32 => next_vec_val!(self, PVf32),
            DataType::Float64 => next_vec_val!(self, PVf64),
            DataType::Utf8 => next_vec_val!(self, UV),
            _ => unimplemented!(),
        };
        if val.is_some() {
            self.index += 1;
        }
        val
    }
}

pub struct FxArcVec<'a>(&'a ArcVec);

pub struct FxBoxVec<'a>(&'a BoxVec);

impl<'a> IntoIterator for FxArcVec<'a> {
    type Item = FxValue;
    type IntoIter = FxVecIntoIterator<'a, ArcVec>;

    fn into_iter(self) -> Self::IntoIter {
        FxVecIntoIterator::<ArcVec>::new(self.0).unwrap()
    }
}

impl<'a> IntoIterator for FxBoxVec<'a> {
    type Item = FxValue;
    type IntoIter = FxVecIntoIterator<'a, BoxVec>;

    fn into_iter(self) -> Self::IntoIter {
        FxVecIntoIterator::<BoxVec>::new(self.0).unwrap()
    }
}

// ================================================================================================
// Generic Iter for types impled FxSeq
// ================================================================================================

pub trait FxIntoIter<T: FxSeq> {
    type Iter<'a>
    where
        Self: 'a;

    fn into_iter(&self) -> Self::Iter<'_>;
}

impl FxIntoIter<ArcArr> for ArcArr {
    type Iter<'a> = FxArrIntoIterator<'a, ArcArr>;

    fn into_iter(&self) -> Self::Iter<'_> {
        FxArrIntoIterator::<ArcArr>::new(&self).unwrap()
    }
}
impl FxIntoIter<BoxArr> for BoxArr {
    type Iter<'a> = FxArrIntoIterator<'a, BoxArr>;

    fn into_iter(&self) -> Self::Iter<'_> {
        FxArrIntoIterator::<BoxArr>::new(&self).unwrap()
    }
}
impl FxIntoIter<ArcVec> for ArcVec {
    type Iter<'a> = FxVecIntoIterator<'a, ArcVec>;

    fn into_iter(&self) -> Self::Iter<'_> {
        FxVecIntoIterator::<ArcVec>::new(&self).unwrap()
    }
}
impl FxIntoIter<BoxVec> for BoxVec {
    type Iter<'a> = FxVecIntoIterator<'a, BoxVec>;

    fn into_iter(&self) -> Self::Iter<'_> {
        FxVecIntoIterator::<BoxVec>::new(&self).unwrap()
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_seq {
    use crate::ab::FromSlice;
    use crate::{arc_arr, arc_vec, box_arr, box_vec};

    use super::*;

    #[test]
    fn into_iter_success() {
        let aa = arc_arr!([Some(1u8), None, Some(3)]);
        let ba = box_arr!([Some(1u8), None, Some(3)]);
        let av = arc_vec!([Some(1u8), None, Some(3)]);
        let bv = box_vec!([Some(1u8), None, Some(3)]);

        let iter_aa = aa.into_iter();
        let iter_ba = ba.into_iter();
        let iter_av = av.into_iter();
        let iter_bv = bv.into_iter();

        println!("len: {:?}", aa.len());
        iter_aa.for_each(|e| println!("> {:?}", e));
        println!("\n");
        println!("len: {:?}", ba.len());
        iter_ba.for_each(|e| println!("> {:?}", e));
        println!("\n");
        println!("len: {:?}", av.len());
        iter_av.for_each(|e| println!("> {:?}", e));
        println!("\n");
        println!("len: {:?}", bv.len());
        iter_bv.for_each(|e| println!("> {:?}", e));
        println!("\n");
    }
}
