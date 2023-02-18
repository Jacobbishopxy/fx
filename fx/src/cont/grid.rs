//! FxGrid

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use ref_cast::RefCast;

use crate::cont::ab::{private, Chunking};
use crate::{FxError, FxResult};

// ================================================================================================
// FxGrid
// ================================================================================================

#[derive(Debug, Clone, RefCast)]
#[repr(transparent)]
pub struct FxGrid(pub(crate) Chunk<Arc<dyn Array>>);

impl private::InnerChunking for FxGrid {
    fn empty() -> Self {
        FxGrid(Chunk::new(vec![]))
    }

    fn ref_chunk(&self) -> &Chunk<Arc<dyn Array>> {
        &self.0
    }

    fn set_chunk(&mut self, arrays: Vec<Arc<dyn Array>>) -> FxResult<()> {
        self.0 = Chunk::new(arrays);
        Ok(())
    }

    fn take_chunk(self) -> Chunk<Arc<dyn Array>> {
        self.0
    }
}

impl FxGrid {
    // WARNING: arrays with different length will cause runtime panic!!!
    pub fn new(arrays: Vec<Arc<dyn Array>>) -> Self {
        FxGrid(Chunk::new(arrays))
    }

    pub fn try_new(arrays: Vec<Arc<dyn Array>>) -> FxResult<Self> {
        let chunk = Chunk::try_new(arrays)?;
        Ok(FxGrid(chunk))
    }

    pub fn gen_schema(&self, names: &[&str]) -> FxResult<Schema> {
        let (al, nl) = (self.width(), names.len());
        if al != nl {
            return Err(FxError::LengthMismatch(al, nl));
        }

        let fld = names
            .iter()
            .zip(self.arrays())
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), a.null_count() > 0))
            .collect::<Vec<_>>();

        Ok(Schema::from(fld))
    }
}

// ================================================================================================
// FxGridColWiseBuilder
// ================================================================================================

// #[derive(Debug)]
// pub struct FxGridColWiseBuilder<const S: usize> {
//     buffer: [Option<FxArray>; S],
// }

// impl<const S: usize> Default for FxGridColWiseBuilder<S> {
//     fn default() -> Self {
//         Self {
//             buffer: [(); S].map(|_| None),
//         }
//     }
// }

// impl<const S: usize> FxGridColWiseBuilder<S> {
//     pub fn new() -> Self {
//         Self::default()
//     }

//     pub fn stack<T: Into<FxArray>>(&mut self, arr: T) -> &mut Self {
//         for e in self.buffer.iter_mut() {
//             if e.is_none() {
//                 *e = Some(arr.into());
//                 break;
//             }
//         }

//         self
//     }

//     pub fn build(self) -> FxResult<FxGrid> {
//         let vec = self.buffer.into_iter().flatten().collect::<Vec<_>>();
//         FxGrid::try_from(vec)
//     }
// }

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_grid {
    // use crate::cont::ab::*;

    // use super::*;

    // #[test]
    // fn grid_builder_col_wise_success() {
    //     let mut builder = FxGridColWiseBuilder::<3>::new();

    //     builder.stack(vec!["a", "b", "c"]);
    //     builder.stack(vec![1, 2, 3]);
    //     builder.stack(vec![Some(1.2), None, Some(2.1)]);

    //     let d = builder.build().unwrap();

    //     println!("{d:?}");
    // }

    // #[test]
    // fn grid_builder_row_wise_proc_macro_success() {
    //     use crate::FX;

    //     #[allow(dead_code)]
    //     #[derive(FX)]
    //     #[fx(FxGrid)]
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

    //     // 3. generate `FxGrid` from builder
    //     let mut bd = Users::gen_chunking_row_builder();

    //     bd.stack(r1).stack(r2);

    //     let d = bd.build();

    //     println!("{d:?}");
    // }

    // #[test]
    // fn concat_should_be_successful() {
    //     let mut builder = FxGridColWiseBuilder::<3>::new();

    //     builder.stack(vec!["a", "b", "c"]);
    //     builder.stack(vec![1, 2, 3]);
    //     builder.stack(vec![Some(1.2), None, Some(2.1)]);

    //     let mut d1 = builder.build().unwrap();
    //     println!("{:?}", d1.data_types());

    //     let mut builder = FxGridColWiseBuilder::<3>::new();

    //     builder.stack(vec!["d", "e"]);
    //     builder.stack(vec![4, 5]);
    //     builder.stack(vec![Some(3.3), None]);

    //     let d2 = builder.build().unwrap();
    //     println!("{:?}", d2.data_types());

    //     let mut builder = FxGridColWiseBuilder::<3>::new();

    //     builder.stack(vec!["f"]);
    //     builder.stack(vec![6]);
    //     builder.stack(vec![Some(4.1)]);

    //     let d3 = builder.build().unwrap();
    //     println!("{:?}", d3.data_types());

    //     let res = d1.concat(&[d2, d3]);

    //     assert!(res.is_ok());

    //     println!("{res:?}");
    // }
}
