//! FX
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

mod macros;

pub mod cont;
pub mod ctor;
pub mod error;
pub mod io;
mod types;
pub mod value;

use macros::*;

pub use cont::batch::*;
pub use cont::bundle::*;
pub use cont::cvt::*;
pub use cont::grid::*;
pub use cont::nullopt::*;
pub use cont::parcel::*;
pub use cont::table::*;

pub use error::*;
pub use fx_macros::FX;
pub use io::arvo::*;
pub use io::csv::*;
pub use io::ipc::*;
pub use io::parquet::*;
pub use io::sql::*;
pub use io::FxIO;
pub use value::*;

pub use arrow2::*;

// ================================================================================================
// Public traits
// ================================================================================================

pub trait FromSlice<T, D> {
    fn from_slice(slice: &[T]) -> D;
}

pub trait FromVec<T, D> {
    fn from_vec(vec: Vec<T>) -> D;
}
