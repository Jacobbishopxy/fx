//! file: rbatch.rs
//! author: Jacob Xie
//! date: 2023/01/20 12:36:42 Friday
//! brief: Batch

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

use crate::Datagrid;

pub struct FxBatch {
    pub(crate) schema: Schema,
    pub(crate) data: Chunk<Arc<dyn Array>>,
}
