//! file: arrow_compute_test.rs
//! author: Jacob Xie
//! date: 2023/01/15 15:50:23 Sunday
//! brief: arrow2::compute

use std::ops::Deref;

use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;

use fx::ab::FromVec;
use fx::{ArcArr, ChunkArr};

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
