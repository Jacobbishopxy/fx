//! file: ctor.rs
//! author: Jacob Xie
//! date: 2023/02/18 11:13:15 Saturday
//! brief:

use crate::macros::*;

// ================================================================================================
// Impl
// ================================================================================================

arr_impl_from_native!(u8);
arr_impl_from_native!(u16);
arr_impl_from_native!(u32);
arr_impl_from_native!(u64);
arr_impl_from_native!(i8);
arr_impl_from_native!(i16);
arr_impl_from_native!(i32);
arr_impl_from_native!(i64);
arr_impl_from_native!(f32);
arr_impl_from_native!(f64);
vec_impl_from_native!(u8);
vec_impl_from_native!(u16);
vec_impl_from_native!(u32);
vec_impl_from_native!(u64);
vec_impl_from_native!(i8);
vec_impl_from_native!(i16);
vec_impl_from_native!(i32);
vec_impl_from_native!(i64);
vec_impl_from_native!(f32);
vec_impl_from_native!(f64);

arr_impl_from_str!(&str);
vec_impl_from_str!(&str);
arr_vec_impl_from_str_slice!();
arr_impl_from_str!(String);
vec_impl_from_str!(String);
arr_vec_impl_from_string_slice!();

arr_impl_from_bool!();
vec_impl_from_bool!();

// ================================================================================================
// Macro
// ================================================================================================

#[macro_export]
macro_rules! arc_arr {
    ($slice:expr) => {
        $crate::cont::ArcArr::from_slice($slice)
    };
}

#[macro_export]
macro_rules! box_arr {
    ($slice:expr) => {
        $crate::cont::BoxArr::from_slice($slice)
    };
}

#[macro_export]
macro_rules! arc_vec {
    ($slice:expr) => {
        $crate::cont::ArcVec::from_slice($slice)
    };
}

#[macro_export]
macro_rules! box_vec {
    ($slice:expr) => {
        $crate::cont::BoxVec::from_slice($slice)
    };
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_ctor {
    use crate::ab::{FromSlice, FromVec};
    use crate::cont::{ArcArr, ArcVec, BoxArr, BoxVec};

    #[test]
    fn arc_arr_from_is_successful() {
        let a1 = ArcArr::from_slice([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = ArcArr::from_slice([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = ArcArr::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = ArcArr::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");

        let a1 = BoxArr::from_slice([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = BoxArr::from_slice([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = BoxArr::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = BoxArr::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_vec_from_is_successful() {
        let a1 = ArcVec::from_slice([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = ArcVec::from_slice([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = ArcVec::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = ArcVec::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");

        let a1 = BoxVec::from_slice([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = BoxVec::from_slice([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = BoxVec::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = BoxVec::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_arr_str_from_is_successful() {
        let a1 = ArcArr::from_slice([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = ArcArr::from_slice(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = ArcArr::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = ArcArr::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");

        let a1 = BoxArr::from_slice([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = BoxArr::from_slice(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = BoxArr::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = BoxArr::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_vec_str_from_is_successful() {
        let a1 = ArcVec::from_slice([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = ArcVec::from_slice(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = ArcVec::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = ArcVec::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");

        let a1 = BoxVec::from_slice([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = BoxVec::from_slice(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = BoxVec::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = BoxVec::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }

    #[test]
    fn macro_arr_success() {
        let a1 = arc_arr!([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = arc_arr!([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = arc_arr!(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = arc_arr!(vec![1u8, 2, 3]);
        println!("{a4:?}");

        let a1 = box_arr!([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = box_arr!([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = box_arr!(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = box_arr!(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn macro_vec_success() {
        let a1 = arc_vec!([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = arc_vec!(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = arc_vec!(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = arc_vec!(vec!["a", "x", "z"]);
        println!("{a4:?}");

        let a1 = box_vec!([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = box_vec!(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = box_vec!(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = box_vec!(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }
}
