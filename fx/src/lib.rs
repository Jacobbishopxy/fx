//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

pub mod connector;
pub mod datagrid;
pub mod error;

pub use connector::*;
pub use datagrid::*;
pub use error::*;
