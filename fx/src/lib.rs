//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

mod macros;

pub mod containers;
pub mod cvt;
pub mod error;
pub mod io;
mod types;
pub mod value;

use macros::*;

pub use containers::*;
pub use error::*;
pub use fx_macros::FX;
pub use io::*;
pub use value::*;

pub use arrow2::*;

// ================================================================================================
// Public traits
// ================================================================================================

pub trait FromSlice<T, D> {
    fn from_slice(slice: &[T]) -> D;
}
