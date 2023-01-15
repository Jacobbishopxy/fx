//! Datagrid

use std::ops::Deref;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Field, Schema};

use crate::{FxArray, FxError, FxResult};

// ================================================================================================
// Datagrid
// ================================================================================================

#[derive(Debug, Clone)]
pub struct Datagrid(pub(crate) Chunk<Arc<dyn Array>>);

impl Datagrid {
    pub fn empty() -> Self {
        Datagrid(Chunk::new(vec![]))
    }

    // WARNING: arrays with different length will cause runtime panic!!!
    pub fn new(arrays: Vec<Arc<dyn Array>>) -> Self {
        Datagrid(Chunk::new(arrays))
    }

    pub fn try_new(arrays: Vec<Arc<dyn Array>>) -> FxResult<Self> {
        let chunk = Chunk::try_new(arrays)?;
        Ok(Datagrid(chunk))
    }

    pub fn data_types(&self) -> Vec<&DataType> {
        self.0.iter().map(|e| e.data_type()).collect()
    }

    pub fn data_types_match(&self, d: &Datagrid) -> bool {
        self.width() == d.width() && self.data_types() == d.data_types()
    }

    pub fn gen_schema(&self, names: &[&str]) -> FxResult<Schema> {
        let arrays = self.arrays();
        let al = arrays.len();
        let nl = names.len();
        if al != nl {
            return Err(FxError::InvalidArgument(format!(
                "length does not match: names.len ${nl} & arrays.len ${al}"
            )));
        }

        let fld = names
            .iter()
            .zip(arrays)
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), a.null_count() > 0))
            .collect::<Vec<_>>();

        Ok(Schema::from(fld))
    }

    pub fn arrays(&self) -> &[Arc<dyn Array>] {
        self.0.arrays()
    }

    pub fn into_arrays(self) -> Vec<Arc<dyn Array>> {
        self.0.into_arrays()
    }

    pub fn length(&self) -> usize {
        self.0.len()
    }

    pub fn width(&self) -> usize {
        self.0.iter().count()
    }

    // (length, width)
    pub fn size(&self) -> (usize, usize) {
        (self.length(), self.width())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn slice() {
        todo!()
    }

    pub fn extend(&mut self, d: &Datagrid) -> FxResult<&mut Self> {
        self.concat(&[d.clone()])
    }

    pub fn concat(&mut self, d: &[Datagrid]) -> FxResult<&mut Self> {
        // check schema integrity
        let mut iter = d.iter();
        while let Some(e) = iter.next() {
            if !self.data_types_match(e) {
                return Err(FxError::SchemaMismatch);
            }
        }

        // original data as column, lift each column into a vector
        // in order to store further column from the input data
        let mut cols = self
            .arrays()
            .iter()
            .map(|e| vec![e.deref()])
            .collect::<Vec<_>>();

        // iterate through input data
        for chunk in d.iter() {
            // mutate columns by appending chunk columns
            cols.iter_mut()
                .zip(chunk.arrays().iter())
                .for_each(|(c, e)| c.push(e.deref()));
        }

        // concatenate each columns
        let mut concated = Vec::<Arc<dyn Array>>::new();
        for c in cols {
            concated.push(Arc::from(concatenate(&c)?));
        }

        self.0 = Chunk::new(concated);

        Ok(self)
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

        println!("{:?}", res);
    }
}
