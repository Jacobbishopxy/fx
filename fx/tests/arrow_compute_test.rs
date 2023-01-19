//! file: arrow_compute_test.rs
//! author: Jacob Xie
//! date: 2023/01/15 15:50:23 Sunday
//! brief: arrow2::compute

use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;

use fx::FxArray;

#[test]
fn concat_success() {
    let a1 = FxArray::from(vec![1u8, 4]);
    let b1 = FxArray::from(vec![true, false]);
    let a2 = FxArray::from(vec![2u8, 5, 6]);
    let b2 = FxArray::from(vec![false, false, true]);

    let chunk1 = Chunk::new(vec![a1.array(), b1.array()]);
    let chunk2 = Chunk::new(vec![a2.array(), b2.array()]);
    let cct1 = concatenate(&[*chunk1.get(0).unwrap(), *chunk2.get(0).unwrap()]).unwrap();
    let cct2 = concatenate(&[*chunk1.get(1).unwrap(), *chunk2.get(1).unwrap()]).unwrap();

    println!("{cct1:?}");
    println!("{cct2:?}");

    let chunk = Chunk::new(vec![cct1, cct2]);
    println!("{chunk:?}");
}
