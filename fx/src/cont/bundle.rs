//! file: bundle.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Bundle

use arrow2::datatypes::{DataType, Field, Schema};

use crate::cont::ab::private;
use crate::types::ArcVec;
use crate::{FxBatch, FxError, FxResult, NullableOptions};

#[derive(Debug, Clone)]
pub struct FxBundle {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<FxBatch>,
}

// impl private::InnerEclecticCollection<usize, FxBatch> for FxBundle {
//     fn empty() -> Self
//     where
//         Self: Sized,
//     {
//         todo!()
//     }

//     fn ref_schema(&self) -> &Schema {
//         todo!()
//     }

//     fn ref_container(&self) -> Vec<&C> {
//         todo!()
//     }

//     fn get_chunk(&self, key: I) -> FxResult<&C> {
//         todo!()
//     }

//     fn get_mut_chunk(&mut self, key: I) -> FxResult<&mut C> {
//         todo!()
//     }

//     fn insert_chunk_type_unchecked(&mut self, key: I, data: C) -> FxResult<()> {
//         todo!()
//     }

//     fn remove_chunk(&mut self, key: I) -> FxResult<()> {
//         todo!()
//     }

//     fn push_chunk_type_unchecked(&mut self, data: C) -> FxResult<()> {
//         todo!()
//     }

//     fn pop_chunk(&mut self) -> FxResult<()> {
//         todo!()
//     }

//     fn take_container(self) -> Vec<C> {
//         todo!()
//     }
// }

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_bundle {
    // use super::*;

    // use crate::cont::ab::*;
    // use crate::{FromSlice, FxArray, FxGrid};

    // #[test]
    // fn new_fx_batches() {
    //     let arrays = vec![
    //         FxArray::from_slice(&["a", "c", "z"]).into_array(),
    //         FxArray::from(vec![Some("x"), None, Some("y")]).into_array(),
    //         FxArray::from_slice(&[true, false, false]).into_array(),
    //     ];
    //     let data = FxGrid::new(arrays);

    //     let batches =
    //         FxBundle::try_new(["c1", "c2", "c3"], data, NullableOptions::indexed_true([2]));

    //     assert!(batches.is_ok());

    //     println!("{:?}", batches.unwrap());
    // }

    // #[test]
    // fn grid_builder_row_wise_proc_macro_success() {
    //     use crate::FX;

    //     #[allow(dead_code)]
    //     #[derive(FX)]
    //     struct Users {
    //         id: i32,
    //         name: String,
    //         check: Option<bool>,
    //     }

    //     let r1 = Users {
    //         id: 1,
    //         name: "Jacob".to_string(),
    //         check: Some(true),
    //     };

    //     let r2 = Users {
    //         id: 2,
    //         name: "Mia".to_string(),
    //         check: None,
    //     };

    //     let mut bd = Users::gen_container_row_builder().unwrap();

    //     bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

    //     let d = bd.build();

    //     println!("{d:?}");
    // }
}
