//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder

use std::hash::Hash;

use crate::ab::{Eclectic, EclecticCollection};
use crate::error::FxResult;

// ================================================================================================
// FxEclecticRowBuilder & FxEclecticRowBuilderGenerator
//
// Based on a named struct, generate a new struct with several vector fields, and each of them
// denotes its original data type (`Option` is supported).
// And this process has been concluded in `fx-macros`, which used procedure macro to auto generate
// all the required implementations for a struct who represents a schema.
// ================================================================================================

pub trait FxEclecticRowBuilder<R, T>: Send
where
    T: Eclectic,
{
    fn new() -> Self
    where
        Self: Sized;

    fn stack(&mut self, row: R) -> &mut Self;

    fn build(self) -> FxResult<T>;
}

pub trait FxEclecticRowBuilderGenerator<T>
where
    Self: Sized,
    T: Eclectic,
{
    type Builder: FxEclecticRowBuilder<Self, T>;

    fn gen_eclectic_row_builder() -> Self::Builder;
}

// ================================================================================================
// FxEclecticCollectionRowBuilder & FxEclecticCollectionRowBuilderGenerator
// ================================================================================================

pub trait FxEclecticCollectionRowBuilder<const SCHEMA: bool, B, R, T, I, C>: Send
where
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    fn new() -> FxResult<Self>
    where
        Self: Sized;

    fn stack(&mut self, row: R) -> &mut Self;

    fn save(&mut self) -> FxResult<&mut Self>;

    fn build(self) -> T;
}

pub trait FxEclecticCollectionRowBuilderGenerator<const SCHEMA: bool, B, R, T, I, C>
where
    Self: Sized,
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    type Builder: FxEclecticCollectionRowBuilder<SCHEMA, B, R, T, I, C>;

    fn gen_eclectic_collection_row_builder() -> FxResult<Self::Builder>;
}

// This test mod is a prototype for derived proc-macro.
#[cfg(test)]
mod test_builder {

    use crate::cont::{ArcArr, ChunkArr};
    use crate::row_builder::*;

    #[allow(dead_code)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    #[derive(Debug, Default)]
    struct UsersEBuild {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
    }

    impl FxEclecticRowBuilder<Users, ChunkArr> for UsersEBuild {
        fn new() -> Self {
            Self::default()
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<ChunkArr> {
            let c1 = ArcArr::from_vec(self.id);
            let c2 = ArcArr::from_vec(self.name);
            let c3 = ArcArr::from_vec(self.check);

            Ok(ChunkArr::try_new(vec![c1, c2, c3])?)
        }
    }

    impl FxEclecticRowBuilderGenerator<ChunkArr> for Users {
        type Builder = UsersEBuild;

        fn gen_eclectic_row_builder() -> Self::Builder {
            UsersEBuild::new()
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
        let mut bd = Users::gen_eclectic_row_builder();

        bd.stack(r1).stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }

    #[derive(Debug)]
    struct UsersCBuild {
        result: Vec<ChunkArr>,
        buffer: Option<UsersEBuild>,
    }

    impl FxEclecticCollectionRowBuilder<false, UsersEBuild, Users, Vec<ChunkArr>, usize, ChunkArr>
        for UsersCBuild
    {
        fn new() -> FxResult<Self>
        where
            Self: Sized,
        {
            let result = Vec::<ChunkArr>::new();
            let buffer = Some(Users::gen_eclectic_row_builder());

            Ok(Self { result, buffer })
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            match self.buffer.as_mut() {
                Some(b) => {
                    b.stack(row);
                }
                None => {
                    let mut buffer = Users::gen_eclectic_row_builder();
                    buffer.stack(row);
                    self.buffer = Some(buffer);
                }
            };

            self
        }

        fn save(&mut self) -> FxResult<&mut Self> {
            let caa = self.buffer.take().unwrap().build()?;
            self.result.push(caa);

            Ok(self)
        }

        fn build(self) -> Vec<ChunkArr> {
            self.result
        }
    }

    impl
        FxEclecticCollectionRowBuilderGenerator<
            false,
            UsersEBuild,
            Users,
            Vec<ChunkArr>,
            usize,
            ChunkArr,
        > for Users
    {
        type Builder = UsersCBuild;

        fn gen_eclectic_collection_row_builder() -> FxResult<Self::Builder> {
            UsersCBuild::new()
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

        let mut bd = Users::gen_eclectic_collection_row_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}

/// another builder test case for schemed container
#[cfg(test)]
mod test_schemed_builder {

    use crate::cont::{ArcArr, FxBatch};
    use crate::row_builder::*;

    #[allow(dead_code)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    #[derive(Debug, Default)]
    struct UsersEBuild {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
    }

    impl FxEclecticRowBuilder<Users, FxBatch> for UsersEBuild {
        fn new() -> Self {
            Self::default()
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<FxBatch> {
            let c1 = ArcArr::from_vec(self.id);
            let c2 = ArcArr::from_vec(self.name);
            let c3 = ArcArr::from_vec(self.check);

            FxBatch::try_new_with_names(vec![c1, c2, c3], ["c1", "c2", "c3"])
        }
    }

    impl FxEclecticRowBuilderGenerator<FxBatch> for Users {
        type Builder = UsersEBuild;

        fn gen_eclectic_row_builder() -> Self::Builder {
            UsersEBuild::new()
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
        let mut bd = Users::gen_eclectic_row_builder();

        bd.stack(r1).stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }
}

#[cfg(test)]
mod test_schemed_container_builder {

    use arrow2::datatypes::DataType;

    use crate::cont::{ArcArr, ChunkArr, FxBatches, NullableOptions};
    use crate::row_builder::*;

    // This part is the same as `test_builder`'s first part.

    #[allow(dead_code)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    #[derive(Debug, Default)]
    struct UsersEBuild {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
    }

    impl FxEclecticRowBuilder<Users, ChunkArr> for UsersEBuild {
        fn new() -> Self {
            Self::default()
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<ChunkArr> {
            let c1 = ArcArr::from_vec(self.id);
            let c2 = ArcArr::from_vec(self.name);
            let c3 = ArcArr::from_vec(self.check);

            Ok(ChunkArr::try_new(vec![c1, c2, c3])?)
        }
    }

    impl FxEclecticRowBuilderGenerator<ChunkArr> for Users {
        type Builder = UsersEBuild;

        fn gen_eclectic_row_builder() -> Self::Builder {
            UsersEBuild::new()
        }
    }

    #[derive(Debug)]
    struct UsersCSBuild {
        result: FxBatches,
        buffer: Option<UsersEBuild>,
    }

    impl FxEclecticCollectionRowBuilder<true, UsersEBuild, Users, FxBatches, usize, ChunkArr>
        for UsersCSBuild
    {
        fn new() -> FxResult<Self>
        where
            Self: Sized,
        {
            let schema = NullableOptions::indexed_true([2]).gen_schema(
                ["id", "name", "check"],
                [DataType::Int32, DataType::Utf8, DataType::Boolean],
            )?;
            let result = FxBatches::empty_with_schema(schema);
            let buffer = Some(Users::gen_eclectic_row_builder());

            Ok(Self { result, buffer })
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            match self.buffer.as_mut() {
                Some(b) => {
                    b.stack(row);
                }
                None => {
                    let mut buffer = Users::gen_eclectic_row_builder();
                    buffer.stack(row);
                    self.buffer = Some(buffer);
                }
            };

            self
        }

        fn save(&mut self) -> FxResult<&mut Self> {
            let caa = self.buffer.take().unwrap().build()?;
            self.result.push(caa)?;

            Ok(self)
        }

        fn build(self) -> FxBatches {
            self.result
        }
    }

    impl
        FxEclecticCollectionRowBuilderGenerator<
            true,
            UsersEBuild,
            Users,
            FxBatches,
            usize,
            ChunkArr,
        > for Users
    {
        type Builder = UsersCSBuild;

        fn gen_eclectic_collection_row_builder() -> FxResult<Self::Builder> {
            UsersCSBuild::new()
        }
    }

    #[test]
    fn schema_container_builder_success() {
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
}
