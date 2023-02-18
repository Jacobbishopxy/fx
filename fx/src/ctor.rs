//! file: ctor.rs
//! author: Jacob Xie
//! date: 2023/02/18 11:13:15 Saturday
//! brief:

use crate::macros::*;
use crate::types::*;

// ================================================================================================
// Impl
// ================================================================================================

arc_arr_impl_from_native!(u8);
arc_arr_impl_from_native!(u16);
arc_arr_impl_from_native!(u32);
arc_arr_impl_from_native!(u64);
arc_arr_impl_from_native!(i8);
arc_arr_impl_from_native!(i16);
arc_arr_impl_from_native!(i32);
arc_arr_impl_from_native!(i64);
arc_arr_impl_from_native!(f32);
arc_arr_impl_from_native!(f64);
arc_vec_impl_from_native!(u8);
arc_vec_impl_from_native!(u16);
arc_vec_impl_from_native!(u32);
arc_vec_impl_from_native!(u64);
arc_vec_impl_from_native!(i8);
arc_vec_impl_from_native!(i16);
arc_vec_impl_from_native!(i32);
arc_vec_impl_from_native!(i64);
arc_vec_impl_from_native!(f32);
arc_vec_impl_from_native!(f64);

arc_arr_impl_from_str!(&str);
arc_vec_impl_from_str!(&str);
arc_arr_impl_from_str!(String);
arc_vec_impl_from_str!(String);

arc_arr_impl_from_bool!();
arc_vec_impl_from_bool!();

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_ctor {
    use super::*;
    use crate::{FromSlice, FromVec};

    #[test]
    fn arc_arr_from_is_successful() {
        let a1 = ArcArr::from_slice(&[Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = ArcArr::from_slice(&[1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = ArcArr::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = ArcArr::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_vec_from_is_successful() {
        let a1 = ArcVec::from_slice(&[Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = ArcVec::from_slice(&[1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = ArcVec::from_vec(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = ArcVec::from_vec(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_arr_str_from_is_successful() {
        let a1 = ArcArr::from_slice(&[Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = ArcArr::from_slice(&["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = ArcArr::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = ArcArr::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }

    #[test]
    fn arc_vec_str_from_is_successful() {
        let a1 = ArcVec::from_slice(&[Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = ArcVec::from_slice(&["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = ArcVec::from_vec(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = ArcVec::from_vec(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }
}
