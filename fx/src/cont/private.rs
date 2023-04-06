//! file: macros.rs
//! author: Jacob Xie
//! date: 2023/02/20 20:11:13 Monday
//! brief: Macros for cont

// ================================================================================================
// Helper macro
// ================================================================================================

pub(crate) mod macros {
    // used for converting Array into MutableArray
    macro_rules! arc_arr_to_vec {
        ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
            let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

            let mba = $arrow_ma::from_iter(arr);

            Ok(::std::sync::Arc::new(mba))
        }};
    }

    macro_rules! box_arr_to_vec {
        ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
            let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

            let mba = $arrow_ma::from_iter(arr);

            Ok(::std::boxed::Box::new(mba))
        }};
    }

    // used for converting Array into MutableArray (primitive type)
    macro_rules! arc_arr_to_vec_p {
        ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
            let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

            let mba = $arrow_ma::from_trusted_len_iter(arr);

            Ok(::std::sync::Arc::new(mba))
        }};
    }

    macro_rules! box_arr_to_vec_p {
        ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
            let arr = $arr.as_typed::<$dwn_cst_r>()?.into_iter();

            let mba = $arrow_ma::from_trusted_len_iter(arr);

            Ok(::std::boxed::Box::new(mba))
        }};
    }

    pub(crate) use arc_arr_to_vec;
    pub(crate) use arc_arr_to_vec_p;
    pub(crate) use box_arr_to_vec;
    pub(crate) use box_arr_to_vec_p;

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
}

// ================================================================================================
// Utils
// ================================================================================================

use arrow2::compute::concatenate::concatenate;

use crate::cont::{ArcArr, BoxArr};
use crate::error::{FxError, FxResult};

pub(crate) fn chop_arc_arr(arr: &ArcArr, len: usize) -> FxResult<(ArcArr, ArcArr)> {
    if len > arr.len() {
        return Err(FxError::OutBounds);
    }

    let l = arr.sliced(0, len);
    let r = arr.sliced(len, arr.len() - len);

    Ok((l.into(), r.into()))
}

pub(crate) fn chop_box_arr(arr: &BoxArr, len: usize) -> FxResult<(BoxArr, BoxArr)> {
    if len > arr.len() {
        return Err(FxError::OutBounds);
    }

    let l = arr.sliced(0, len);
    let r = arr.sliced(len, arr.len() - len);

    Ok((l, r))
}

pub(crate) fn concat_arc_arr(arrs: &[ArcArr]) -> FxResult<ArcArr> {
    let arrs = arrs.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    Ok(concatenate(&arrs)?.into())
}

pub(crate) fn concat_box_arr(arrs: &[BoxArr]) -> FxResult<BoxArr> {
    let arrs = arrs.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    Ok(concatenate(&arrs)?)
}

#[cfg(test)]
mod test_private {
    use super::*;
    use crate::ab::FromSlice;
    use crate::{arc_arr, box_arr};

    #[test]
    fn chop_array_success() {
        let a = arc_arr!([1, 2, 3, 4, 5, 6]);
        let (l, r) = chop_arc_arr(&a, 4).unwrap();

        println!("{:?}", a);
        println!("{:?}", l);
        println!("{:?}", r);

        let b = box_arr!([1, 2, 3, 4, 5, 6]);
        let (l, r) = chop_box_arr(&b, 4).unwrap();

        println!("{:?}", b);
        println!("{:?}", l);
        println!("{:?}", r);
    }

    #[test]
    fn concat_array_success() {
        let arrs = vec![arc_arr!([1, 2, 3]), arc_arr!([3, 2, 1]), arc_arr!([5, 6])];
        let res = concat_arc_arr(&arrs).unwrap();
        println!("{:?}", res);

        let arrs = vec![box_arr!([1, 2, 3]), box_arr!([3, 2, 1]), box_arr!([5, 6])];
        let res = concat_box_arr(&arrs).unwrap();
        println!("{:?}", res);
    }
}
