//! file: fx_seq_test.rs
//! author: Jacob Xie
//! date: 2023/03/03 22:27:54 Friday
//! brief:

use fx::ab::{FromSlice, FxSeq};
use fx::array::*;
use fx::cont::{ArcArr, ArcVec};

#[test]
fn downcast_test() {
    let arc_arr_bool = ArcArr::from_slice(&[true, false, true]);

    println!("{:?}", arc_arr_bool.data_type());

    let bool_arr = arc_arr_bool.as_typed::<BooleanArray>().unwrap();

    for i in bool_arr {
        println!("{:?}", i);
    }
}
