//! author: Jacob Xie
//! date: 2023/02/19 22:11:02 Sunday
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]

mod macros;

pub mod cont;
pub mod ctor;
pub mod error;
pub mod io;
pub mod types;
pub mod value;

use macros::*;
// pub use fx_macros::FX;
pub use error::*;
pub use types::*;
pub use value::*;

// cont
pub use cont::batch::*;
pub use cont::bundle::*;
pub use cont::cvt::*;
pub use cont::nullopt::*;
pub use cont::parcel::*;
pub use cont::table::*;

// io
pub use io::arvo::*;
pub use io::csv::*;
pub use io::ipc::*;
pub use io::parquet::*;
pub use io::sql::*;
pub use io::FxIO;

// re-export
pub use arrow2::*;

// ================================================================================================
// Crate namespace ab
// ================================================================================================

// reexport all ab, so that can use all the traits in ab as `use fx::ab::*`
pub mod ab {
    pub use crate::cont::ab::*;
    pub use crate::io::ab::*;

    pub trait FromSlice<T, D> {
        fn from_slice(slice: &[T]) -> D;
    }

    pub trait FromVec<T, D> {
        fn from_vec(vec: Vec<T>) -> D;
    }
}
