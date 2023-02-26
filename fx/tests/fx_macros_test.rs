//! file: fx_macros_test.rs
//! author: Jacob Xie
//! date: 2023/02/21 20:23:56 Tuesday
//! brief:

// use fx::cont::*;
// use fx::row_builder::*;

// #[test]
// fn builder_row_wise1_proc_macro_success() {
//     #[derive(FX, Clone)]
//     struct Users {
//         id: i32,
//         name: String,
//         check: Option<bool>,
//     }

//     #[allow(dead_code)]
//     let r1 = Users {
//         id: 1,
//         name: "Jacob".to_string(),
//         check: Some(true),
//     };

//     let r2 = Users {
//         id: 2,
//         name: "Mia".to_string(),
//         check: None,
//     };

//     let mut bd1 = Users::gen_eclectic_row_builder();
//     bd1.stack(r1.clone()).stack(r2.clone());
//     // Chunk<Arc<dyn Array>>
//     let d = bd1.build();
//     println!("{d:?}");

//     let mut bd = Users::gen_collection_row_builder().unwrap();
//     bd.stack(r1).save().unwrap().stack(r2).save().unwrap();
//     let d = bd.build();
//     println!("{d:?}");
// }

// #[test]
// fn builder_row_wise2_proc_macro_success() {
//     #[derive(FX)]
//     #[fx(table)]
//     struct Users {
//         id: i32,
//         name: String,
//         check: Option<bool>,
//     }

//     #[allow(dead_code)]
//     let r1 = Users {
//         id: 1,
//         name: "Jacob".to_string(),
//         check: Some(true),
//     };

//     let r2 = Users {
//         id: 2,
//         name: "Mia".to_string(),
//         check: None,
//     };

//     let mut bd1 = Users::gen_eclectic_row_builder();
//     bd1.stack(r1).stack(r2);
//     // FxTable<Arc<dyn Array>>
//     let d = bd1.build();
//     println!("{d:?}");
// }
