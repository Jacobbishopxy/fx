//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

mod macros;

pub mod array;
pub mod connector;
pub mod datagrid;
pub mod error;
pub mod value;
pub mod vector;

use macros::*;

pub use array::*;
pub use connector::*;
pub use datagrid::*;
pub use error::*;
pub use fx_macros::FX;
pub use value::*;
pub use vector::*;

// ================================================================================================
// Public traits
// ================================================================================================

pub trait FromSlice<T, D> {
    fn from_slice(slice: &[T]) -> D;
}
