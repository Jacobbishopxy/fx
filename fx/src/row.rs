//! Row

use crate::{Datagrid, FxResult, Value};

// ================================================================================================
// Row
// ================================================================================================

pub struct Row<const S: usize>([Value; S]);

// ================================================================================================
// DatagridRowBuilder
// ================================================================================================

pub struct DatagridRawBuilder<const S: usize> {
    buffer: Vec<Row<S>>,
}

impl<const S: usize> Default for DatagridRawBuilder<S> {
    fn default() -> Self {
        Self { buffer: Vec::new() }
    }
}

impl<const S: usize> DatagridRawBuilder<S> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self) -> FxResult<&mut Self> {
        todo!()
    }

    pub fn stack(&mut self) -> FxResult<()> {
        todo!()
    }

    pub fn build(self) -> FxResult<Datagrid> {
        todo!()
    }
}
