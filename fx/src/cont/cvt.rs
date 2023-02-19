//! file: cvt.rs
//! author: Jacob Xie
//! date: 2023/01/14 18:58:04 Saturday
//! brief: Convertion between FxArray and FxVector

// use std::sync::Arc;

// use arrow2::array::{Array, MutableArray};
// use arrow2::datatypes::DataType;
// use ref_cast::RefCast;

// use crate::cont::ab::Chunking;
// use crate::macros::invalid_len;
// use crate::types::*;
// use crate::{FxError, FxGrid, FxResult};

// ================================================================================================
//  Conversion between FxVector & FxArray
// ================================================================================================

// ================================================================================================
// FxGrid & FxArray conversions
// ================================================================================================

// impl TryFrom<Vec<FxArray>> for FxGrid {
//     type Error = FxError;

//     fn try_from(value: Vec<FxArray>) -> Result<Self, Self::Error> {
//         invalid_len!(value);

//         Ok(FxGrid::new(value.into_iter().map(|e| e.0).collect()))
//     }
// }

// impl From<FxGrid> for Vec<FxArray> {
//     fn from(d: FxGrid) -> Self {
//         d.into_arrays().into_iter().map(FxArray).collect()
//     }
// }

// ================================================================================================
// FxGrid & FxVector conversions
// ================================================================================================

// impl TryFrom<Vec<FxVector>> for FxGrid {
//     type Error = FxError;

//     fn try_from(value: Vec<FxVector>) -> Result<Self, Self::Error> {
//         invalid_len!(value);

//         let mut vec_arr = vec![];
//         for e in value.into_iter() {
//             vec_arr.push(FxArray::try_from(e)?.0)
//         }

//         Ok(FxGrid::new(vec_arr))
//     }
// }

// impl From<FxGrid> for Vec<FxVector> {
//     fn from(d: FxGrid) -> Self {
//         d.into_arrays()
//             .into_iter()
//             .map(|e| FxVector::try_from(FxArray(e)))
//             .collect::<FxResult<Vec<_>>>()
//             .expect("From FxGrid to Vec<FxVector> should always success")
//     }
// }

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_cvt {
    // use crate::FromSlice;

    // use super::*;

    // #[test]
    // fn try_from_array_to_vector() {
    //     let arr1 = FxArray::from_slice(&[true, false]);
    //     let res1 = FxVector::try_from(arr1);

    //     let arr2 = FxArray::from_slice(&[1i8, 2, 3]);
    //     let res2 = FxVector::try_from(arr2);

    //     let arr3 = FxArray::from_slice(&[1f32, 2.0, 3.0]);
    //     let res3 = FxVector::try_from(arr3);

    //     let arr4 = FxArray::from_slice(&["a", "b", "c"]);
    //     let res4 = FxVector::try_from(arr4);

    //     assert!(res1.is_ok());
    //     assert!(res2.is_ok());
    //     assert!(res3.is_ok());
    //     assert!(res4.is_ok());
    // }
}
