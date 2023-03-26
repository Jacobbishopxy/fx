//! file: ctor.rs
//! author: Jacob Xie
//! date: 2023/02/18 11:13:15 Saturday
//! brief:

use crate::ab::FromSlice;
use crate::macros::*;
use crate::prelude::{ArcArr, ArcVec};

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

// &str
impl<'a, S: AsRef<[&'a str]>> FromSlice<S, [&'a str], ArcArr> for ArcArr {
    fn from_slice(slice: S) -> ArcArr {
        arrow2::array::Utf8Array::<i32>::from_slice(slice.as_ref()).arced()
    }
}

// &str
impl<'a, S: AsRef<[Option<&'a str>]>> FromSlice<S, [Option<&'a str>], ArcArr> for ArcArr {
    fn from_slice(slice: S) -> ArcArr {
        let vec = slice.as_ref().to_vec();
        arrow2::array::Utf8Array::<i32>::from(vec).arced()
    }
}

// &str
impl<'a, S: AsRef<[&'a str]>> FromSlice<S, [&'a str], ArcVec> for ArcVec {
    fn from_slice(slice: S) -> ArcVec {
        std::sync::Arc::new(arrow2::array::MutableUtf8Array::<i32>::from_iter_values(
            slice.as_ref().iter(),
        ))
    }
}

// &str
impl<'a, S: AsRef<[Option<&'a str>]>> FromSlice<S, [Option<&'a str>], ArcVec> for ArcVec {
    fn from_slice(slice: S) -> ArcVec {
        let vec = slice.as_ref().to_vec();
        std::sync::Arc::new(arrow2::array::MutableUtf8Array::<i32>::from(vec))
    }
}

// String
impl<S: AsRef<[String]>> FromSlice<S, [String], ArcArr> for ArcArr {
    fn from_slice(slice: S) -> ArcArr {
        arrow2::array::Utf8Array::<i32>::from_slice(slice.as_ref()).arced()
    }
}

// String
impl<S: AsRef<[Option<String>]>> FromSlice<S, [Option<String>], ArcArr> for ArcArr {
    fn from_slice(slice: S) -> ArcArr {
        let vec = slice.as_ref().to_vec();
        arrow2::array::Utf8Array::<i32>::from(vec).arced()
    }
}

// String
impl<S: AsRef<[String]>> FromSlice<S, [String], ArcVec> for ArcVec {
    fn from_slice(slice: S) -> ArcVec {
        std::sync::Arc::new(arrow2::array::MutableUtf8Array::<i32>::from_iter_values(
            slice.as_ref().iter(),
        ))
    }
}

// String
impl<S: AsRef<[Option<String>]>> FromSlice<S, [Option<String>], ArcVec> for ArcVec {
    fn from_slice(slice: S) -> ArcVec {
        let vec = slice.as_ref().to_vec();
        std::sync::Arc::new(arrow2::array::MutableUtf8Array::<i32>::from(vec))
    }
}

arc_arr_impl_from_bool!();
arc_vec_impl_from_bool!();

// ================================================================================================
// Macro
// ================================================================================================

#[macro_export]
macro_rules! aar {
    ($slice:expr) => {
        $crate::cont::ArcArr::from_slice($slice)
    };
}

#[macro_export]
macro_rules! avc {
    ($slice:expr) => {
        $crate::cont::ArcVec::from_slice($slice)
    };
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_ctor {
    use crate::ab::{FromSlice, FromVec};
    use crate::cont::{ArcArr, ArcVec};

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
    }

    #[test]
    fn macro_aar_success() {
        let a1 = aar!([Some(1u8), None, Some(3)]);
        println!("{a1:?}");
        let a2 = aar!([1u8, 2, 3]);
        println!("{a2:?}");
        let a3 = aar!(vec![Some(1u8), None, Some(3)]);
        println!("{a3:?}");
        let a4 = aar!(vec![1u8, 2, 3]);
        println!("{a4:?}");
    }

    #[test]
    fn macro_avc_success() {
        let a1 = avc!([Some("a"), None, Some("c")]);
        println!("{a1:?}");
        let a2 = avc!(["a", "d", "h"]);
        println!("{a2:?}");
        let a3 = avc!(vec![Some("x"), None, Some("y")]);
        println!("{a3:?}");
        let a4 = avc!(vec!["a", "x", "z"]);
        println!("{a4:?}");
    }
}
