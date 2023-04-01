//! file: parallel.rs
//! author: Jacob Xie
//! date: 2023/04/01 19:30:31 Saturday
//! brief:

use std::io::Write;
// use std::sync::Arc;

// use futures::io::AsyncWrite;

// use super::{AsyncReadSeek};
use super::ReadSeek;
use crate::cont::FxTabular;
use crate::error::FxResult;

#[derive(Default)]
pub struct ParallelIO {
    pub(crate) data: Option<FxTabular>,
    pub(crate) writer: Option<Box<dyn Write>>,
    pub(crate) reader: Option<Box<dyn ReadSeek>>,
    // pub(crate) async_writer: Option<Arc<dyn AsyncWrite>>,
    // pub(crate) async_reader: Option<Arc<dyn AsyncReadSeek>>,
}

impl ParallelIO {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_data(data: FxTabular) -> Self {
        Self {
            data: Some(data),
            ..Default::default()
        }
    }

    pub fn data(&self) -> Option<&FxTabular> {
        self.data.as_ref()
    }

    pub fn task_data(&mut self) -> Option<FxTabular> {
        self.data.take()
    }

    pub fn set_file_writer(&mut self, path: &str) -> FxResult<()> {
        self.writer = Some(Box::new(std::fs::File::create(path)?));
        Ok(())
    }

    pub fn set_file_reader(&mut self, path: &str) -> FxResult<()> {
        self.reader = Some(Box::new(std::fs::File::open(path)?));
        Ok(())
    }
}
