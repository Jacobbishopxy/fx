//! file: mod.rs
//! author: Jacob Xie
//! date: 2023/01/30 10:52:38 Monday
//! brief: Abstract traits and etc.

pub mod builder;
pub mod chunking;
pub mod container;
pub(crate) mod private;
pub mod seq;
pub mod sheaf;

pub use builder::*;
pub use chunking::*;
pub use container::*;
pub use seq::*;
pub use sheaf::*;

// ================================================================================================
// Helper macro
// ================================================================================================

macro_rules! arr_to_vec {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr
            .as_any()
            .downcast_ref::<$dwn_cst_r>()
            .ok_or($crate::FxError::FailedToConvert)?
            .into_iter();

        let mba = $arrow_ma::from_iter(arr);

        Ok(::std::sync::Arc::new(mba))
    }};
}

macro_rules! arr_to_vec_p {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr
            .as_any()
            .downcast_ref::<$dwn_cst_r>()
            .ok_or($crate::FxError::FailedToConvert)?
            .into_iter();

        let mba = $arrow_ma::from_trusted_len_iter(arr);

        Ok(::std::sync::Arc::new(mba))
    }};
}

pub(crate) use arr_to_vec;
pub(crate) use arr_to_vec_p;
