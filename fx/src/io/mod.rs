//! file: mod.rs
//! author: Jacob Xie
//! date: 2023/01/18 20:02:41 Wednesday
//! brief: I/O

pub mod ab;
pub mod arvo;
pub mod csv;
pub mod ec;
pub mod ipc;
pub mod parquet;
pub mod sql;

pub use arvo::*;
pub use csv::*;
pub use ec::*;
pub use ipc::*;
pub use parquet::*;
pub use sql::*;

/// A public struct used for implementing serval type of I/O
pub struct FxIO;
