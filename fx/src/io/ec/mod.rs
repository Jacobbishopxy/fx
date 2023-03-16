//! file: mod.rs
//! author: Jacob Xie
//! date: 2023/03/15 23:18:39 Wednesday
//! brief:

pub mod parallel;
pub mod simple;

pub use parallel::*;
pub use simple::*;

use std::io::{Read, Seek};

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}
