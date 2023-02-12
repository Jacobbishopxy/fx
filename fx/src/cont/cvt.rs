//! file: cvt.rs
//! author: Jacob Xie
//! date: 2023/01/14 18:58:04 Saturday
//! brief: Convertion between FxArray and FxVector

use std::sync::Arc;

use arrow2::array::{Array, MutableArray};
use arrow2::datatypes::DataType;
use ref_cast::RefCast;

use crate::cont::ab::Chunking;
use crate::macros::{arr_to_vec_branch, arr_to_vec_p_branch, invalid_len};
use crate::types::*;
use crate::{FxArray, FxError, FxGrid, FxResult, FxVector};

// ================================================================================================
//  Conversion between FxVector & FxArray
// ================================================================================================

impl TryFrom<FxVector> for FxArray {
    type Error = FxError;

    fn try_from(mut vector: FxVector) -> Result<Self, Self::Error> {
        let arr = Arc::get_mut(&mut vector.0)
            .ok_or(FxError::FailedToConvert)?
            .as_arc();

        Ok(FxArray(arr))
    }
}

impl TryFrom<FxArray> for FxVector {
    type Error = FxError;

    fn try_from(array: FxArray) -> Result<Self, Self::Error> {
        match array.data_type() {
            DataType::Boolean => arr_to_vec_branch!(array, BA, MB),
            DataType::Int8 => arr_to_vec_p_branch!(array, PAi8, MPAi8),
            DataType::Int16 => arr_to_vec_p_branch!(array, PAi16, MPAi16),
            DataType::Int32 => arr_to_vec_p_branch!(array, PAi32, MPAi32),
            DataType::Int64 => arr_to_vec_p_branch!(array, PAi64, MPAi64),
            DataType::UInt8 => arr_to_vec_p_branch!(array, PAu8, MPAu8),
            DataType::UInt16 => arr_to_vec_p_branch!(array, PAu16, MPAu16),
            DataType::UInt32 => arr_to_vec_p_branch!(array, PAu32, MPAu32),
            DataType::UInt64 => arr_to_vec_p_branch!(array, PAu64, MPAu64),
            DataType::Float32 => arr_to_vec_p_branch!(array, PAf32, MPAf32),
            DataType::Float64 => arr_to_vec_p_branch!(array, PAf64, MPAf64),
            DataType::Utf8 => arr_to_vec_branch!(array, UA, MU),
            _ => Err(FxError::FailedToConvert),
        }
    }
}

// ================================================================================================
// AsRef & AsMut
// ================================================================================================

impl AsRef<FxArray> for Arc<dyn Array> {
    fn as_ref(&self) -> &FxArray {
        FxArray::ref_cast(self)
    }
}

impl AsRef<FxVector> for Arc<dyn MutableArray> {
    fn as_ref(&self) -> &FxVector {
        FxVector::ref_cast(self)
    }
}

impl AsMut<FxArray> for Arc<dyn Array> {
    fn as_mut(&mut self) -> &mut FxArray {
        FxArray::ref_cast_mut(self)
    }
}

impl AsMut<FxVector> for Arc<dyn MutableArray> {
    fn as_mut(&mut self) -> &mut FxVector {
        FxVector::ref_cast_mut(self)
    }
}

// ================================================================================================
// FxGrid & FxArray conversions
// ================================================================================================

impl TryFrom<Vec<FxArray>> for FxGrid {
    type Error = FxError;

    fn try_from(value: Vec<FxArray>) -> Result<Self, Self::Error> {
        invalid_len!(value);

        Ok(FxGrid::new(value.into_iter().map(|e| e.0).collect()))
    }
}

impl From<FxGrid> for Vec<FxArray> {
    fn from(d: FxGrid) -> Self {
        d.into_arrays().into_iter().map(FxArray).collect()
    }
}

// ================================================================================================
// FxGrid & FxVector conversions
// ================================================================================================

impl TryFrom<Vec<FxVector>> for FxGrid {
    type Error = FxError;

    fn try_from(value: Vec<FxVector>) -> Result<Self, Self::Error> {
        invalid_len!(value);

        let mut vec_arr = vec![];
        for e in value.into_iter() {
            vec_arr.push(FxArray::try_from(e)?.0)
        }

        Ok(FxGrid::new(vec_arr))
    }
}

impl From<FxGrid> for Vec<FxVector> {
    fn from(d: FxGrid) -> Self {
        d.into_arrays()
            .into_iter()
            .map(|e| FxVector::try_from(FxArray(e)))
            .collect::<FxResult<Vec<_>>>()
            .expect("From FxGrid to Vec<FxVector> should always success")
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_cvt {
    use crate::FromSlice;

    use super::*;

    #[test]
    fn try_from_array_to_vector() {
        let arr1 = FxArray::from_slice(&[true, false]);
        let res1 = FxVector::try_from(arr1);

        let arr2 = FxArray::from_slice(&[1i8, 2, 3]);
        let res2 = FxVector::try_from(arr2);

        let arr3 = FxArray::from_slice(&[1f32, 2.0, 3.0]);
        let res3 = FxVector::try_from(arr3);

        let arr4 = FxArray::from_slice(&["a", "b", "c"]);
        let res4 = FxVector::try_from(arr4);

        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());
        assert!(res4.is_ok());
    }
}
