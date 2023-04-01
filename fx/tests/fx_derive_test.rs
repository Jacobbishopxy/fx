//! file: fx_macros_test.rs
//! author: Jacob Xie
//! date: 2023/02/21 20:23:56 Tuesday
//! brief:

use fx::cont::*;
use fx::prelude::*;

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
    let d = bd1.build(); // FxBatch
    println!("{d:?}");
}

#[test]
fn fx_bundle_builder_success() {
    #[derive(FX, Clone)]
    #[fx(bundle)]
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

    let mut bd1 = Users::gen_bundle_builder();
    bd1.stack(r1.clone()).stack(r2.clone());
    let d1 = bd1.build(); // FxBundle<Arc<dyn Array>, 3>
    assert!(d1.is_ok());
    println!("{:?}", d1.unwrap());

    let mut bd2 = Users::gen_arraa_builder();
    bd2.stack(r1).stack(r2);
    let d2 = bd2.build(); // [Arc<dyn Aarry>; 3]
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
    let res = bd.build(); // FxBatch
    assert!(res.is_ok());
    println!("{:?}", res.unwrap());
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

    let mut bd1 = Users::gen_arraa_builder();
    bd1.stack(r1.clone()).stack(r2.clone());
    let d1 = bd1.build(); // [Arc<dyn Aarry>; 3]
    assert!(d1.is_ok());
    println!("{:?}", d1.unwrap());

    let mut bd2 = Users::gen_chunk_builder();
    bd2.stack(r1.clone()).stack(r2.clone());
    let d2 = bd2.build();
    assert!(d2.is_ok());
    println!("{:?}", d2.unwrap());

    let mut bd3 = Users::gen_batch_builder();
    bd3.stack(r1).stack(r2);
    let d3 = bd3.build();
    assert!(d3.is_ok());
    println!("{:?}", d3.unwrap());

    let cbd1 = Users::gen_arraa_table_builder();
    assert!(cbd1.is_ok());

    let cbd2 = Users::gen_chunk_table_builder();
    assert!(cbd2.is_ok());

    let cbd3 = Users::gen_batch_table_builder();
    assert!(cbd3.is_ok());
}

#[test]
fn fx_tabular_builder_success() {
    #[derive(FX, Clone)]
    #[fx(tabular)]
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

    let mut bd1 = Users::gen_arraa_builder();
    bd1.stack(r1.clone()).stack(r2.clone());
    let d1 = bd1.build(); // [Arc<dyn Aarry>; 3]
    assert!(d1.is_ok());
    println!("{:?}", d1.unwrap());

    let mut bd2 = Users::gen_chunk_builder();
    bd2.stack(r1.clone()).stack(r2.clone());
    let d2 = bd2.build();
    assert!(d2.is_ok());
    println!("{:?}", d2.unwrap());

    let mut bd3 = Users::gen_batch_builder();
    bd3.stack(r1).stack(r2);
    let d3 = bd3.build();
    assert!(d3.is_ok());
    println!("{:?}", d3.unwrap());

    let cbd1 = Users::gen_arraa_tabular_builder();
    assert!(cbd1.is_ok());

    let cbd2 = Users::gen_chunk_tabular_builder();
    assert!(cbd2.is_ok());

    let cbd3 = Users::gen_batch_tabular_builder();
    assert!(cbd3.is_ok());
}
