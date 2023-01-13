//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

pub mod array;
pub mod connector;
pub mod datagrid;
pub mod error;
mod macros;
pub mod vector;

pub use array::*;
pub use connector::*;
pub use datagrid::*;
pub use error::*;
pub use fx_macros::FX;
use macros::*;
pub use vector::*;

// ================================================================================================
// Public traits
// ================================================================================================

pub trait FromSlice<T, D> {
    fn from_slice(slice: &[T]) -> D;
}
