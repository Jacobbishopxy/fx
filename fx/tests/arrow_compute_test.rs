//! file: arrow_compute_test.rs
//! author: Jacob Xie
//! date: 2023/01/15 15:50:23 Sunday
//! brief: arrow2::compute

use std::ops::Deref;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::compute::arithmetics as dyn_ari;
use arrow2::compute::arithmetics::basic::*;
use arrow2::compute::arity::{binary, unary};
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;

use fx::ab::{AsArray, FromSlice, FromVec};
use fx::arc_arr;
use fx::cont::{ArcArr, ChunkArr};
use fx::types::*;

#[test]
fn concat_success() {
    let a1 = ArcArr::from_vec(vec![1u8, 4]);
    let b1 = ArcArr::from_vec(vec![true, false]);
    let a2 = ArcArr::from_vec(vec![2u8, 5, 6]);
    let b2 = ArcArr::from_vec(vec![false, false, true]);

    let chunk1 = ChunkArr::new(vec![a1, b1]);
    let chunk2 = ChunkArr::new(vec![a2, b2]);
    let cct1 = concatenate(&[
        chunk1.get(0).unwrap().deref(),
        chunk2.get(0).unwrap().deref(),
    ])
    .unwrap();
    let cct2 = concatenate(&[
        chunk1.get(1).unwrap().deref(),
        chunk2.get(1).unwrap().deref(),
    ])
    .unwrap();

    println!("{cct1:?}");
    println!("{cct2:?}");

    let chunk = Chunk::new(vec![cct1, cct2]);
    println!("{chunk:?}");
}

#[test]
fn add_success() {
    let a1 = arc_arr!([1i64, 2, 3]);
    let a2 = arc_arr!([Some(4i64), None, Some(6)]);

    let added = add(a1.as_i64_arr_unchecked(), a2.as_i64_arr_unchecked());
    println!("added: {:?}", added);

    assert_eq!(added, PAi64::from(&[Some(5), None, Some(9)]));
}

#[test]
fn subtract_success() {
    let a1 = arc_arr!([1i64, 2, 3]);
    let a2 = arc_arr!([Some(4i64), None, Some(6)]);

    let subtracted = sub(a1.as_i64_arr_unchecked(), a2.as_i64_arr_unchecked());
    println!("subtracted: {:?}", subtracted);

    assert_eq!(subtracted, PAi64::from(&[Some(-3), None, Some(-3)]));
}

#[test]
fn add_a_scalar_success() {
    let a1 = arc_arr!([1i64, 2, 3]);

    let plus10 = add_scalar(a1.as_i64_arr_unchecked(), &10);
    println!("add_a_scalar: {:?}", plus10);

    assert_eq!(plus10, PAi64::from(&[Some(11), Some(12), Some(13)]));
}

#[test]
fn dyn_add_success() {
    let a1 = arc_arr!([1i64, 2, 3]);
    let a2 = arc_arr!([Some(4i64), None, Some(6)]);

    assert!(dyn_ari::can_add(a1.data_type(), a2.data_type()));

    let added = dyn_ari::add(a1.as_ref(), a2.as_ref());
    println!("added: {:?}", added.as_ref());

    assert_eq!(added.as_ref(), PAi64::from(&[Some(5), None, Some(9)]));
}

#[test]
fn unary_op_success() {
    let a1 = arc_arr!([Some(4.0f64), None, Some(6.0)]);
    let r = unary(
        a1.as_f64_arr_unchecked(),
        |x| x.cos().powi(2) + x.sin().powi(2),
        DataType::Float64,
    );
    println!("{:?}", r);

    assert!((r.value(0) - 1.0).abs() < 0.0001);
    assert!(r.is_null(1));
    assert!((r.value(2) - 1.0).abs() < 0.0001);
}

#[test]
fn binary_op_success() {
    let a1 = arc_arr!([1i64, 2, 3]);
    let a2 = arc_arr!([Some(4i64), None, Some(6)]);

    let op = |x: i64, y: i64| x.pow(2) + y.pow(2);
    let r = binary(
        a1.as_i64_arr_unchecked(),
        a2.as_i64_arr_unchecked(),
        DataType::Int64,
        op,
    );
    println!("{:?}", r);

    assert_eq!(r, PAi64::from(&[Some(1 + 16), None, Some(9 + 36)]));
}
