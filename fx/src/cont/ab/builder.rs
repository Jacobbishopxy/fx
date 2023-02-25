//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder

use std::hash::Hash;
use std::marker::PhantomData;

use crate::ab::{Eclectic, EclecticCollection};
use crate::error::{FxError, FxResult};

use super::private;

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

pub struct FxEclecticRowBuilderGenerator<R, T>
where
    T: Eclectic,
{
    _row_type: PhantomData<R>,
    _eclectic_type: PhantomData<T>,
}

impl<R, T> FxEclecticRowBuilderGenerator<R, T>
where
    T: Eclectic,
{
    fn gen_eclectic_row_builder<B>() -> B
    where
        B: FxEclecticRowBuilder<R, T>,
    {
        B::new()
    }
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

pub struct FxCollectionRowBuilderGenerator<const SCHEMA: bool, B, R, T, I, C>
where
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    // _has_schema: PhantomData<SCHEMA>,
    _eclectic_row_builder: PhantomData<B>,
    _row_type: PhantomData<R>,
    _collection_type: PhantomData<T>,
    _index_type: PhantomData<I>,
    _eclectic_type: PhantomData<C>,
}

impl<const SCHEMA: bool, B, R, T, I, C> FxCollectionRowBuilderGenerator<SCHEMA, B, R, T, I, C>
where
    B: FxEclecticRowBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
{
    fn gen_collection_row_builder<CB>() -> FxResult<CB>
    where
        CB: FxCollectionRowBuilder<SCHEMA, B, R, T, I, C>,
    {
        CB::new()
    }
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
        let mut bd = FxEclecticRowBuilderGenerator::<Users, ChunkArr>::gen_eclectic_row_builder::<
            UsersEBuild,
        >();

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
            let buffer = Some(
                FxEclecticRowBuilderGenerator::<Users, ChunkArr>::gen_eclectic_row_builder::<
                    UsersEBuild,
                >(),
            );

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

        let mut bd = FxCollectionRowBuilderGenerator::<
            false,
            UsersEBuild,
            Users,
            Vec<ChunkArr>,
            usize,
            ChunkArr,
        >::gen_collection_row_builder::<UsersCBuild>()
        .unwrap();

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
        let mut bd = FxEclecticRowBuilderGenerator::<Users, FxBatch>::gen_eclectic_row_builder::<
            UsersEBuild,
        >();

        bd.stack(r1).stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }
}

#[cfg(test)]
mod test_multiple_schemed_container_builder {

    use std::marker::PhantomData;

    use arrow2::datatypes::DataType;

    use crate::cont::{ArcArr, ChunkArr, FxBatch, FxBatches, NullableOptions};
    use crate::row_builder::*;

    // This part is the same as `test_builder`'s first part.

    #[allow(dead_code)]
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    #[derive(Debug)]
    struct UsersEBuild<T: Eclectic> {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
        _e: PhantomData<T>,
    }

    // the first impl: ChunkArr
    impl FxEclecticRowBuilder<Users, ChunkArr> for UsersEBuild<ChunkArr> {
        fn new() -> Self {
            Self {
                id: Vec::<i32>::new(),
                name: Vec::<String>::new(),
                check: Vec::<Option<bool>>::new(),
                _e: PhantomData,
            }
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

    // the second impl: FxBatch
    impl FxEclecticRowBuilder<Users, FxBatch> for UsersEBuild<FxBatch> {
        fn new() -> Self {
            Self {
                id: Vec::<i32>::new(),
                name: Vec::<String>::new(),
                check: Vec::<Option<bool>>::new(),
                _e: PhantomData,
            }
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

    #[derive(Debug)]
    struct UsersCSBuild {
        result: FxBatches<ChunkArr>,
        buffer: Option<UsersEBuild<ChunkArr>>,
    }

    impl
        FxCollectionRowBuilder<
            true,
            UsersEBuild<ChunkArr>,
            Users,
            FxBatches<ChunkArr>,
            usize,
            ChunkArr,
        > for UsersCSBuild
    {
        fn new() -> FxResult<Self>
        where
            Self: Sized,
        {
            let schema = NullableOptions::indexed_true([2]).gen_schema(
                ["id", "name", "check"],
                [DataType::Int32, DataType::Utf8, DataType::Boolean],
            )?;
            let result = FxBatches::<ChunkArr>::empty_with_schema(schema);
            let bf = FxEclecticRowBuilderGenerator::<Users, ChunkArr>::gen_eclectic_row_builder::<
                UsersEBuild<ChunkArr>,
            >();
            let buffer = Some(bf);

            Ok(Self { result, buffer })
        }

        fn mut_buffer(&mut self) -> Option<&mut UsersEBuild<ChunkArr>> {
            self.buffer.as_mut()
        }

        fn set_buffer(&mut self, buffer: UsersEBuild<ChunkArr>) {
            self.buffer = Some(buffer);
        }

        fn take_buffer(&mut self) -> Option<UsersEBuild<ChunkArr>> {
            self.buffer.take()
        }

        fn mut_result(&mut self) -> &mut FxBatches<ChunkArr> {
            &mut self.result
        }

        fn take_result(self) -> FxBatches<ChunkArr> {
            self.result
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

        let mut bd = FxCollectionRowBuilderGenerator::<
            true,
            UsersEBuild<ChunkArr>,
            Users,
            FxBatches<ChunkArr>,
            usize,
            ChunkArr,
        >::gen_collection_row_builder::<UsersCSBuild>()
        .unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}
