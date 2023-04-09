//! file: fx_table_tabular_test.rs
//! author: Jacob Xie
//! date: 2023/04/09 21:21:11 Sunday
//! brief:

// use fx::prelude::*;
use fx::ab::{Dqs, FromSlice};
use fx::arc_arr;
use fx::cont::FxTabular;

#[test]
fn table_dqs_trait_test() {
    let d = FxTabular::new(vec![arc_arr!([1, 2, 3]), arc_arr!(["a", "b", "c"])]);

    println!("{:?}", d.deque_lens());

    let arrs = d.into_arrays();
    println!("{:?}", arrs);
}
