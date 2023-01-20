//! file: private.rs
//! author: Jacob Xie
//! date: 2023/01/21 00:55:29 Saturday
//! brief: Private

use std::sync::Arc;

use arrow2::array::Array;
use arrow2::chunk::Chunk;

use crate::FxResult;

// pub(crate) in mod.rs, so that external access is prohibited
#[doc(hidden)]
pub trait InnerChunking
where
    Self: std::marker::Sized,
{
    fn empty() -> Self;

    fn new(arrays: Vec<Arc<dyn Array>>) -> Self;

    fn try_new(arrays: Vec<Arc<dyn Array>>) -> FxResult<Self>;

    fn ref_chunk(&self) -> &Chunk<Arc<dyn Array>>;

    fn set_chunk(&mut self, arrays: Vec<Arc<dyn Array>>) -> FxResult<()>;

    fn take_chunk(self) -> Chunk<Arc<dyn Array>>;
}
