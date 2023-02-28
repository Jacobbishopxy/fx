//! file: fx_macros_test.rs
//! author: Jacob Xie
//! date: 2023/02/21 20:23:56 Tuesday
//! brief:

use fx::cont::*;
use fx::row_builder::*;

#[test]
fn fx_default_builder_success() {
    #[derive(FX, Clone)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

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

    let mut bd1 = Users::gen_batch_builder();
    bd1.stack(r1).stack(r2);
    let d = bd1.build(); // Chunk<Arc<dyn Array>>
    println!("{d:?}");
}

#[test]
fn fx_table_builder_success() {
    #[derive(FX, Clone)]
    #[fx(table)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

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

    let mut bd1 = Users::gen_table_builder();
    bd1.stack(r1.clone()).stack(r2.clone());
    let d1 = bd1.build(); // FxTable<Arc<dyn Array>>
    assert!(d1.is_ok());
    println!("{:?}", d1.unwrap());

    let mut bd2 = Users::gen_arraa_builder();
    bd2.stack(r1).stack(r2);
    let d2 = bd2.build();
    assert!(d2.is_ok());
    println!("{:?}", d2.unwrap());
}

#[test]
fn fx_fall_to_default_success() {
    #[derive(FX, Clone)]
    #[fx(wrong_attr)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

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

    let mut bd = Users::gen_batch_builder();
    bd.stack(r1).stack(r2);
    let res = bd.build();
    assert!(res.is_ok());
    println!("{:?}", res.unwrap());
}
