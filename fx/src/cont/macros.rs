//! file: macros.rs
//! author: Jacob Xie
//! date: 2023/02/20 20:11:13 Monday
//! brief: Macros for cont

// ================================================================================================
// Helper macro
// ================================================================================================

// used for converting Array into MutableArray
macro_rules! arr_to_vec {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

        let mba = $arrow_ma::from_iter(arr);

        Ok(::std::sync::Arc::new(mba))
    }};
}

// used for converting Array into MutableArray (primitive type)
macro_rules! arr_to_vec_p {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

        let mba = $arrow_ma::from_trusted_len_iter(arr);

        Ok(::std::sync::Arc::new(mba))
    }};
}

pub(crate) use arr_to_vec;
pub(crate) use arr_to_vec_p;

// used for MutableArray `concat`
macro_rules! try_ext_from_slf {
    ($arr:expr, $s:expr, $dwn_cst_r:ident) => {{
        let ma = $arr.as_typed_mut::<$dwn_cst_r>()?;

        let s = $s.as_typed::<$dwn_cst_r>()?;

        ma.try_extend_from_self(s)?;

        Ok($arr)
    }};
}

pub(crate) use try_ext_from_slf;