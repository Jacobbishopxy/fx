//! file: mod.rs
//! author: Jacob Xie
//! date: 2023/03/15 23:18:39 Wednesday
//! brief:

pub mod parallel;
pub mod simple;

use futures::{AsyncRead, AsyncSeek};
pub use parallel::*;
pub use simple::*;

use std::io::{Read, Seek};

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek> AsyncReadSeek for T {}
