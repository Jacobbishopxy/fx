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

// derived proc-macro
pub use fx_macros::FX;

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

// an easier way for using `FX` derived proc-macro, see `tests/fx_macros_test.rs`
pub mod row_builder {
    pub use crate::FX;

    pub use crate::ab::*;

    pub use crate::error::FxResult;
}
