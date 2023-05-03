//! file: fx_iter_test.rs
//! author: Jacob Xie
//! date: 2023/03/03 21:44:41 Friday
//! brief:

use fx::array::*;
use fx::prelude::*;

#[test]
fn into_inner_test() {
    // ================================================================================================
    // Arr
    // ================================================================================================

    let p_arr = PrimitiveArray::<i32>::from([Some(1), None, Some(3)]);

    println!("{:?}", p_arr);

    let p_arr_inner = p_arr.into_inner();

    println!("{:?}", p_arr_inner);

    // ================================================================================================
    // Vec
    // ================================================================================================

    let p_vec = MutablePrimitiveArray::<i32>::from([Some(5), None, Some(7)]);

    println!("{:?}", p_vec);

    let p_vec_inner = p_vec.into_inner();

    println!("{:?}", p_vec_inner);
}

#[test]
fn iter_test() {
    // ================================================================================================
    // Arr
    // ================================================================================================

    let p_arr = PrimitiveArray::<i32>::from([Some(1), None, Some(3)]);

    for i in p_arr.iter() {
        println!("{:?}", i);
    }

    // ================================================================================================
    // Vec
    // ================================================================================================

    let p_vec = MutablePrimitiveArray::<i32>::from([Some(5), None, Some(7)]);

    for i in p_vec.iter() {
        println!("{:?}", i);
    }
}

#[test]
fn arc_box_iter_test() {
    // ================================================================================================
    // Arr
    // ================================================================================================

    let aa = arc_arr!([1i8, 2, 3]);
    let iter_aa = aa.into_iter();
    iter_aa.for_each(|e| println!("> {:?}", e));
    println!("\n");

    let aa = arc_arr!([Some(1u8), None, Some(3)]);
    let iter_aa = aa.into_iter();
    iter_aa.for_each(|e| println!("> {:?}", e));
    println!("\n");

    let ba = box_arr!([1i8, 2, 3]);
    let iter_ba = ba.into_iter();
    iter_ba.for_each(|e| println!("> {:?}", e));
    println!("\n");
    let ba = box_arr!([Some(1u8), None, Some(3)]);
    let iter_ba = ba.into_iter();
    iter_ba.for_each(|e| println!("> {:?}", e));
    println!("\n");

    // ================================================================================================
    // Vec
    // ================================================================================================

    let av = arc_vec!([1i8, 2, 3]);
    let iter_av = av.into_iter();
    iter_av.for_each(|e| println!("> {:?}", e));
    println!("\n");

    let av = arc_vec!([Some(1u8), None, Some(3)]);
    let iter_av = av.into_iter();
    iter_av.for_each(|e| println!("> {:?}", e));
    println!("\n");

    let bv = box_vec!([1i8, 2, 3]);
    let iter_bv = bv.into_iter();
    iter_bv.for_each(|e| println!("> {:?}", e));
    println!("\n");
    let bv = box_vec!([Some(1u8), None, Some(3)]);
    let iter_bv = bv.into_iter();
    iter_bv.for_each(|e| println!("> {:?}", e));
    println!("\n");
}
