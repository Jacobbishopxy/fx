//! file: bundle.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Bundle

use arrow2::datatypes::{Field, Schema};

use crate::ab::{private, Purport, StaticPurport};
use crate::{ChunkArr, FxError, FxResult};

#[derive(Debug, Clone)]
pub struct FxBundle {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<ChunkArr>,
}

impl StaticPurport for FxBundle {}

impl Purport for FxBundle {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl private::InnerEclecticCollection<true, usize, ChunkArr> for FxBundle {
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::<ChunkArr>::new(),
        }
    }

    fn ref_schema(&self) -> Option<&Schema> {
        Some(&self.schema)
    }

    fn ref_container(&self) -> Vec<&ChunkArr> {
        self.data.iter().collect()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&ChunkArr> {
        self.data.get(key).ok_or(FxError::OutBounds)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut ChunkArr> {
        self.data.get_mut(key).ok_or(FxError::OutBounds)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: ChunkArr) -> FxResult<()> {
        if key > self.data.len() {
            return Err(FxError::OutBounds);
        }

        self.data.insert(key, data);

        Ok(())
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        if key > self.data.len() {
            return Err(FxError::OutBounds);
        }

        self.data.remove(key);

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: ChunkArr) -> FxResult<()> {
        self.data.push(data);

        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.data.pop();

        Ok(())
    }

    fn take_container(self) -> Vec<ChunkArr> {
        self.data
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_bundle {
    // use super::*;

    // use crate::cont::*;
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
