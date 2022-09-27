//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

pub mod array;
pub mod connector;
pub mod datagrid;
pub mod error;

pub use array::*;
pub use connector::*;
pub use datagrid::*;
pub use error::*;
