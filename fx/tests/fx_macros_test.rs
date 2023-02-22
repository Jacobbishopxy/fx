//! file: fx_macros_test.rs
//! author: Jacob Xie
//! date: 2023/02/21 20:23:56 Tuesday
//! brief:

use fx::cont::{ArcArr, ChunkArr, FxBundle};
use fx::row_builder::*;

#[derive(FX)]
struct Users {
    id: i32,
    name: String,
    check: Option<bool>,
}

#[test]
fn grid_builder_row_wise_proc_macro_success() {
    #[allow(dead_code)]
    let r1 = Users {
        id: 1,
        name: "Jacob".to_string(),
        check: Some(true),
    };

    let r2 = Users {
        id: 2,
        name: "Mia".to_string(),
        check: None,
    };

    let mut bd = Users::gen_eclectic_collection_row_builder().unwrap();

    bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

    let d = bd.build();

    println!("{d:?}");
}
