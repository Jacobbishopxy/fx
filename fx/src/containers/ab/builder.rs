//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder

use crate::{FxGrid, FxResult};

// ================================================================================================
// FxContainerRowBuilderGenerator & FxGridRowBuilder
//
// Based on a named struct, generate a new struct with several vector fields, and each of them
// denotes its original data type (`Option` is supported).
// And this process has been concluded in `fx-macros`, which used procedure macro to auto generate
// all the required implementations for a struct who represents a schema.
// ================================================================================================

// TODO: all containers should have the same generic row-wise builder trait

pub trait FxContainerRowBuilderGenerator {
    fn gen_row_builder() -> Box<dyn FxContainerRowBuilder<Self>>;
}

pub trait FxContainerRowBuilder<T>: Send {
    fn new() -> Self
    where
        Self: Sized;

    fn stack(&mut self, row: T);

    fn build(self: Box<Self>) -> FxResult<FxGrid>;
}

#[cfg(test)]
mod test_builder {
    use crate::FxArray;

    use super::*;

    #[test]
    fn grid_builder_row_wise_success() {
        #[allow(dead_code)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        #[derive(Default)]
        struct UsersBuild {
            id: Vec<i32>,
            name: Vec<String>,
            check: Vec<Option<bool>>,
        }

        impl FxContainerRowBuilder<Users> for UsersBuild {
            fn new() -> Self {
                Self::default()
            }

            fn stack(&mut self, row: Users) {
                self.id.push(row.id);
                self.name.push(row.name);
                self.check.push(row.check);
            }

            fn build(self: Box<Self>) -> FxResult<FxGrid> {
                FxGrid::try_from(vec![
                    FxArray::from(self.id),
                    FxArray::from(self.name),
                    FxArray::from(self.check),
                ])
            }
        }

        impl FxContainerRowBuilderGenerator for Users {
            fn gen_row_builder() -> Box<dyn FxContainerRowBuilder<Self>> {
                Box::new(UsersBuild::new())
            }
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

        // 3. generate `FxGrid` from builder
        let mut bd = Users::gen_row_builder();

        bd.stack(r1);
        bd.stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }
}
