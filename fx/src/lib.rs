//! author: Jacob Xie
//! date: 2023/02/19 22:11:02 Sunday
//!
//! Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix)

#![feature(type_alias_impl_trait)]
#![feature(array_methods)]
#![feature(impl_trait_in_assoc_type)]

mod macros;

pub mod cont;
mod ctor;
pub mod error;
pub mod io;
pub mod types;
pub mod value;

// derived proc-macro
pub use fx_derive::FX;

// re-export
// pub use arrow2::*;

// ================================================================================================
// Crate namespace ab
// ================================================================================================

// reexport all ab, so that can use all the traits in ab as `use fx::ab::*`
pub mod ab {
    pub use super::cont::ab::*;
    pub use super::io::ab::*;

    pub trait FromSlice<S, T, D>
    where
        S: AsRef<T> + ?Sized,
        T: ?Sized,
    {
        fn from_slice(slice: S) -> D;
    }

    pub trait FromVec<T, D> {
        fn from_vec(vec: Vec<T>) -> D;
    }
}

// ================================================================================================
// Crate namespace prelude
// ================================================================================================

// an easier way for using `FX` derived proc-macro, see `tests/fx_macros_test.rs`
pub mod prelude {
    pub use super::FX;

    pub use super::cont::ab::*;
    pub use super::cont::*;
    pub use super::{arc_arr, arc_vec, box_arr, box_vec};

    pub use super::error::*;
    pub use super::types::*;

    pub use super::ab::{FromSlice, FromVec};
}
