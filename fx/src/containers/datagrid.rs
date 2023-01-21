//! Datagrid

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use ref_cast::RefCast;

use crate::chunking::Chunking;
use crate::{private, FxArray, FxError, FxResult};

// ================================================================================================
// Datagrid
// ================================================================================================

#[derive(Debug, Clone, RefCast)]
#[repr(transparent)]
pub struct Datagrid(pub(crate) Chunk<Arc<dyn Array>>);

impl private::InnerChunking for Datagrid {
    fn empty() -> Self {
        Datagrid(Chunk::new(vec![]))
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

impl Datagrid {
    // WARNING: arrays with different length will cause runtime panic!!!
    pub fn new(arrays: Vec<Arc<dyn Array>>) -> Self {
        Datagrid(Chunk::new(arrays))
    }

    pub fn try_new(arrays: Vec<Arc<dyn Array>>) -> FxResult<Self> {
        let chunk = Chunk::try_new(arrays)?;
        Ok(Datagrid(chunk))
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
// DatagridColWiseBuilder
// ================================================================================================

#[derive(Debug)]
pub struct DatagridColWiseBuilder<const S: usize> {
    buffer: [Option<FxArray>; S],
}

impl<const S: usize> Default for DatagridColWiseBuilder<S> {
    fn default() -> Self {
        Self {
            buffer: [(); S].map(|_| None),
        }
    }
}

impl<const S: usize> DatagridColWiseBuilder<S> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stack<T: Into<FxArray>>(&mut self, arr: T) -> &mut Self {
        for e in self.buffer.iter_mut() {
            if e.is_none() {
                *e = Some(arr.into());
                break;
            }
        }

        self
    }

    pub fn build(self) -> FxResult<Datagrid> {
        let vec = self.buffer.into_iter().flatten().collect::<Vec<_>>();
        Datagrid::try_from(vec)
    }
}

// ================================================================================================
// FxDatagrid & FxDatagridRowBuilderCst & FxDatagridRowBuilder
// ================================================================================================

pub trait FxDatagrid {
    fn gen_row_builder() -> Box<dyn FxDatagridRowBuilder<Self>>;
}

pub trait FxDatagridRowBuilderCst {
    fn new() -> Self;
}

pub trait FxDatagridRowBuilder<T>: Send {
    fn stack(&mut self, row: T);

    fn build(self: Box<Self>) -> FxResult<Datagrid>;
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_datagrid {

    use super::*;

    #[test]
    fn datagrid_builder_col_wise_success() {
        let mut builder = DatagridColWiseBuilder::<3>::new();

        builder.stack(vec!["a", "b", "c"]);
        builder.stack(vec![1, 2, 3]);
        builder.stack(vec![Some(1.2), None, Some(2.1)]);

        let d = builder.build().unwrap();

        println!("{d:?}");
    }

    #[test]
    fn datagrid_builder_row_wise_success() {
        #[allow(dead_code)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        #[derive(Default)]
        struct UsersBuild {
            id: Vec<i32>,
            name: Vec<String>,
            check: Vec<Option<bool>>,
        }

        impl FxDatagridRowBuilderCst for UsersBuild {
            fn new() -> Self {
                Self::default()
            }
        }

        impl FxDatagridRowBuilder<Users> for UsersBuild {
            fn stack(&mut self, row: Users) {
                self.id.push(row.id);
                self.name.push(row.name);
                self.check.push(row.check);
            }

            fn build(self: Box<Self>) -> FxResult<Datagrid> {
                Datagrid::try_from(vec![
                    FxArray::from(self.id),
                    FxArray::from(self.name),
                    FxArray::from(self.check),
                ])
            }
        }

        impl FxDatagrid for Users {
            fn gen_row_builder() -> Box<dyn FxDatagridRowBuilder<Self>> {
                Box::new(UsersBuild::new())
            }
        }

        let r1 = Users {
            id: 1,
            name: "Jacob".to_string(),
            check: Some(true),
        };

        let r2 = Users {
            id: 2,
            name: "Mia".to_string(),
            check: None,
        };

        // 3. generate `Datagrid` from builder
        let mut bd = Users::gen_row_builder();

        bd.stack(r1);
        bd.stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }

    #[test]
    fn datagrid_builder_row_wise_proc_macro_success() {
        use crate::FX;

        #[allow(dead_code)]
        #[derive(FX)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        let r1 = Users {
            id: 1,
            name: "Jacob".to_string(),
            check: Some(true),
        };

        let r2 = Users {
            id: 2,
            name: "Mia".to_string(),
            check: None,
        };

        // 3. generate `Datagrid` from builder
        let mut bd = Users::gen_row_builder();

        bd.stack(r1);
        bd.stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }

    #[test]
    fn concat_should_be_successful() {
        let mut builder = DatagridColWiseBuilder::<3>::new();

        builder.stack(vec!["a", "b", "c"]);
        builder.stack(vec![1, 2, 3]);
        builder.stack(vec![Some(1.2), None, Some(2.1)]);

        let mut d1 = builder.build().unwrap();
        println!("{:?}", d1.data_types());

        let mut builder = DatagridColWiseBuilder::<3>::new();

        builder.stack(vec!["d", "e"]);
        builder.stack(vec![4, 5]);
        builder.stack(vec![Some(3.3), None]);

        let d2 = builder.build().unwrap();
        println!("{:?}", d2.data_types());

        let mut builder = DatagridColWiseBuilder::<3>::new();

        builder.stack(vec!["f"]);
        builder.stack(vec![6]);
        builder.stack(vec![Some(4.1)]);

        let d3 = builder.build().unwrap();
        println!("{:?}", d3.data_types());

        let res = d1.concat(&[d2, d3]);

        assert!(res.is_ok());

        println!("{res:?}");
    }
}
