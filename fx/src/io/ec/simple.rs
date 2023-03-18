//! file: simple.rs
//! author: Jacob Xie
//! date: 2023/03/15 23:20:05 Wednesday
//! brief:

use std::io::Write;

use super::ReadSeek;
use crate::ab::{Eclectic, Purport};
use crate::error::FxResult;

#[derive(Default)]
pub struct SimpleIO<T>
where
    T: Eclectic + Purport,
{
    pub(crate) data: Option<T>,
    pub(crate) writer: Option<Box<dyn Write>>,
    pub(crate) reader: Option<Box<dyn ReadSeek>>,
}

impl<T> SimpleIO<T>
where
    T: Eclectic + Purport,
{
    pub fn new() -> Self {
        Self {
            data: None,
            writer: None,
            reader: None,
        }
    }

    pub fn new_with_data(data: T) -> Self {
        Self {
            data: Some(data),
            writer: None,
            reader: None,
        }
    }

    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    pub fn take_data(&mut self) -> Option<T> {
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

    // pub fn set_buf_writer(&mut self) {}

    // pub fn set_buf_reader(&mut self) {}

    // pub fn set_cursor_writer(&mut self) {}

    // pub fn set_cursor_reader(&mut self) {}
}
