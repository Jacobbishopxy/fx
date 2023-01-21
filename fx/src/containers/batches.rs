//! file: batches.rs
//! author: Jacob Xie
//! date: 2023/01/20 22:34:35 Friday
//! brief: Batches

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

pub struct FxBatches {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<Chunk<Arc<dyn Array>>>,
}
