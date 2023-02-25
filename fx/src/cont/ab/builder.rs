//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder

use std::hash::Hash;

use crate::ab::{Eclectic, EclecticCollection};
use crate::error::{FxError, FxResult};

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
// Pro FxCollectionRowBuilder FxCollectionRowBuilderGenerator
// ================================================================================================

pub trait FxCollectionRowBuilder<const SCHEMA: bool, B, R, T, I, C>: Send
where
    Self: Sized,
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    fn new() -> FxResult<Self>
    where
        Self: Sized;

    fn mut_buffer(&mut self) -> Option<&mut B>;

    fn set_buffer(&mut self, buffer: B);

    fn take_buffer(&mut self) -> Option<B>;

    fn mut_result(&mut self) -> &mut T;

    fn take_result(self) -> T;

    fn stack(&mut self, row: R) -> &mut Self {
        match self.mut_buffer() {
            Some(b) => {
                b.stack(row);
            }
            None => {
                let mut buffer = B::new();
                buffer.stack(row);
                self.set_buffer(buffer);
            }
        }

        self
    }

    fn save(&mut self) -> FxResult<&mut Self> {
        let b = self.take_buffer().ok_or(FxError::EmptyContent)?;
        let c = b.build()?;
        self.mut_result().push(c)?;

        Ok(self)
    }

    fn build(self) -> T {
        self.take_result()
    }
}

pub trait FxCollectionRowBuilderGenerator<const SCHEMA: bool, B, R, T, I, C>
where
    Self: Sized,
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    type Builder: FxCollectionRowBuilder<SCHEMA, B, R, T, I, C>;

    fn gen_collection_row_builder() -> FxResult<Self::Builder>;
}

// ================================================================================================
// Test
// ================================================================================================

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

    impl FxCollectionRowBuilder<false, UsersEBuild, Users, Vec<ChunkArr>, usize, ChunkArr>
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

        fn mut_buffer(&mut self) -> Option<&mut UsersEBuild> {
            self.buffer.as_mut()
        }

        fn set_buffer(&mut self, buffer: UsersEBuild) {
            self.buffer = Some(buffer)
        }

        fn take_buffer(&mut self) -> Option<UsersEBuild> {
            self.buffer.take()
        }

        fn mut_result(&mut self) -> &mut Vec<ChunkArr> {
            &mut self.result
        }

        fn take_result(self) -> Vec<ChunkArr> {
            self.result
        }
    }

    impl FxCollectionRowBuilderGenerator<false, UsersEBuild, Users, Vec<ChunkArr>, usize, ChunkArr>
        for Users
    {
        type Builder = UsersCBuild;

        fn gen_collection_row_builder() -> FxResult<Self::Builder> {
            Self::Builder::new()
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

        let mut bd = Users::gen_collection_row_builder().unwrap();

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

    use crate::cont::{ArcArr, FxBatch, FxBatches, NullableOptions};
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

            FxBatch::try_new(vec![c1, c2, c3])
        }
    }

    impl FxEclecticRowBuilderGenerator<FxBatch> for Users {
        type Builder = UsersEBuild;

        fn gen_eclectic_row_builder() -> Self::Builder {
            UsersEBuild::new()
        }
    }

    #[derive(Debug)]
    struct UsersCSBuild {
        result: FxBatches<FxBatch>,
        buffer: Option<UsersEBuild>,
    }

    impl FxCollectionRowBuilder<true, UsersEBuild, Users, FxBatches<FxBatch>, usize, FxBatch>
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
            let result = FxBatches::<FxBatch>::empty_with_schema(schema);
            let buffer = Some(Users::gen_eclectic_row_builder());

            Ok(Self { result, buffer })
        }

        fn mut_buffer(&mut self) -> Option<&mut UsersEBuild> {
            self.buffer.as_mut()
        }

        fn set_buffer(&mut self, buffer: UsersEBuild) {
            self.buffer = Some(buffer);
        }

        fn take_buffer(&mut self) -> Option<UsersEBuild> {
            self.buffer.take()
        }

        fn mut_result(&mut self) -> &mut FxBatches<FxBatch> {
            &mut self.result
        }

        fn take_result(self) -> FxBatches<FxBatch> {
            self.result
        }
    }

    impl
        FxCollectionRowBuilderGenerator<
            true,
            UsersEBuild,
            Users,
            FxBatches<FxBatch>,
            usize,
            FxBatch,
        > for Users
    {
        type Builder = UsersCSBuild;

        fn gen_collection_row_builder() -> FxResult<Self::Builder> {
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

        let mut bd = Users::gen_collection_row_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}
