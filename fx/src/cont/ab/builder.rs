//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder

use std::hash::Hash;

use crate::cont::ab::{Chunking, ChunkingContainer};
use crate::FxResult;

// ================================================================================================
// FxChunkingRowBuilder & FxChunkingRowBuilderGenerator
//
// Based on a named struct, generate a new struct with several vector fields, and each of them
// denotes its original data type (`Option` is supported).
// And this process has been concluded in `fx-macros`, which used procedure macro to auto generate
// all the required implementations for a struct who represents a schema.
// ================================================================================================

pub trait FxChunkingRowBuilder<R, T>: Send
where
    T: Chunking,
{
    fn new() -> Self
    where
        Self: Sized;

    fn stack(&mut self, row: R) -> &mut Self;

    fn build(self) -> FxResult<T>;
}

pub trait FxChunkingRowBuilderGenerator<T>
where
    Self: Sized,
    T: Chunking,
{
    type Builder: FxChunkingRowBuilder<Self, T>;

    fn gen_chunking_row_builder() -> Self::Builder;
}

// ================================================================================================
// FxContainerRowBuilder & FxContainerRowBuilderGenerator
// ================================================================================================

pub trait FxContainerRowBuilder<B, R, T, I, C>: Send
where
    B: FxChunkingRowBuilder<R, C>,
    T: ChunkingContainer<I, C>,
    I: Hash,
    C: Chunking,
{
    fn new() -> FxResult<Self>
    where
        Self: Sized;

    fn stack(&mut self, row: R) -> &mut Self;

    fn save(&mut self) -> FxResult<&mut Self>;

    fn build(self) -> T;
}

pub trait FxContainerRowBuilderGenerator<B, R, T, I, C>
where
    Self: Sized,
    B: FxChunkingRowBuilder<R, C>,
    T: ChunkingContainer<I, C>,
    I: Hash,
    C: Chunking,
{
    type Builder: FxContainerRowBuilder<B, R, T, I, C>;

    fn gen_container_row_builder() -> FxResult<Self::Builder>;
}

#[cfg(test)]
mod test_builder {
    use arrow2::datatypes::{DataType, Field};

    use crate::{FxArray, FxBundle, FxGrid};

    use super::*;

    #[allow(dead_code)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    #[derive(Debug, Default)]
    struct UsersChunkingBuild {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
    }

    impl FxChunkingRowBuilder<Users, FxGrid> for UsersChunkingBuild {
        fn new() -> Self {
            Self::default()
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<FxGrid> {
            FxGrid::try_from(vec![
                FxArray::from(self.id),
                FxArray::from(self.name),
                FxArray::from(self.check),
            ])
        }
    }

    impl FxChunkingRowBuilderGenerator<FxGrid> for Users {
        type Builder = UsersChunkingBuild;

        fn gen_chunking_row_builder() -> Self::Builder {
            UsersChunkingBuild::new()
        }
    }

    #[test]
    fn chunking_builder_success() {
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
        let mut bd = Users::gen_chunking_row_builder();

        bd.stack(r1).stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }

    #[derive(Debug)]
    struct UsersContainerBuild {
        result: FxBundle<FxGrid>,
        buffer: Option<UsersChunkingBuild>,
    }

    impl FxContainerRowBuilder<UsersChunkingBuild, Users, FxBundle<FxGrid>, usize, FxGrid>
        for UsersContainerBuild
    {
        fn new() -> FxResult<Self>
        where
            Self: Sized,
        {
            let fields = vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("check", DataType::Boolean, true),
            ];

            let result = FxBundle::<FxGrid>::new_empty_by_fields(fields)?;
            let buffer = Some(Users::gen_chunking_row_builder());

            Ok(Self { result, buffer })
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            match self.buffer.as_mut() {
                Some(b) => {
                    b.stack(row);
                }
                None => {
                    let mut buffer = Users::gen_chunking_row_builder();
                    buffer.stack(row);
                    self.buffer = Some(buffer);
                }
            };

            self
        }

        fn save(&mut self) -> FxResult<&mut Self> {
            let grid = self.buffer.take().unwrap().build()?;
            self.result.push(grid)?;

            Ok(self)
        }

        fn build(self) -> FxBundle<FxGrid> {
            self.result
        }
    }

    impl FxContainerRowBuilderGenerator<UsersChunkingBuild, Users, FxBundle<FxGrid>, usize, FxGrid>
        for Users
    {
        type Builder = UsersContainerBuild;

        fn gen_container_row_builder() -> FxResult<Self::Builder> {
            UsersContainerBuild::new()
        }
    }

    #[test]
    fn container_builder_success() {
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

        let mut bd = Users::gen_container_row_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}
