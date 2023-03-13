//! file: fx_builder_test.rs
//! author: Jacob Xie
//! date: 2023/02/26 20:33:04 Sunday
//!
//! Three test cases:
//! 1. simple_procedure
//! 2. schemed_builder
//! 3. multiple_schemed_builder

use fx::cont::*;
use fx::row_builder::*;

// This test mod is a prototype for derived proc-macro.
#[cfg(test)]
mod simple_procedure {
    use super::*;

    // 0. a struct as a schema
    struct Users {
        id: i32,
        name: String,
        check: Option<bool>,
    }

    // 1. prepare a builder struct
    #[derive(Debug, Default)]
    struct UsersEBuild {
        id: Vec<i32>,
        name: Vec<String>,
        check: Vec<Option<bool>>,
    }

    // 2. impl builder trait with specified eclectic type for this builder
    impl FxEclecticBuilder<Users, ChunkArr> for UsersEBuild {
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

    // 3. impl builder generator for the schema struct
    impl FxChunkBuilderGenerator for Users {
        type ChunkBuilder = UsersEBuild;
    }

    #[test]
    fn chunk_builder_success() {
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

        let mut bd = Users::gen_chunk_builder();

        bd.stack(r1).stack(r2);

        let d = bd.build();
        assert!(d.is_ok());

        println!("{:?}", d.unwrap());
    }

    // 4. prepare a builder for the collection
    #[derive(Debug)]
    struct UsersCBuild {
        result: Vec<ChunkArr>,
        buffer: Option<UsersEBuild>,
    }

    // 5. impl collection builder trait
    impl FxCollectionBuilder<false, UsersEBuild, Users, Vec<ChunkArr>, usize, ChunkArr>
        for UsersCBuild
    {
        fn new() -> FxResult<Self> {
            let result = Vec::<ChunkArr>::new();
            let buffer = Some(Users::gen_chunk_builder());

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

    // 6. impl builder generator for the schema struct
    impl FxChunksBuilderGenerator for Users {
        type ChunkBuilder = UsersEBuild;

        type ChunksBuilder = UsersCBuild;
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

        let mut bd = Users::gen_chunks_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}

// another builder test case for schemed container
#[cfg(test)]
mod schemed_builder {

    use fx::datatypes::DataType;

    use super::*;

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

    impl FxEclecticBuilder<Users, FxBatch> for UsersEBuild {
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

    impl FxBatchBuilderGenerator for Users {
        type BatchBuilder = UsersEBuild;
    }

    #[test]
    fn batch_builder_success() {
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

        let d = bd.build();
        assert!(d.is_ok());

        println!("{:?}", d.unwrap());
    }

    #[derive(Debug)]
    struct UsersCBuild {
        result: FxBatches<FxBatch>,
        buffer: Option<UsersEBuild>,
    }

    // 5. impl collection builder trait
    impl FxCollectionBuilder<true, UsersEBuild, Users, FxBatches<FxBatch>, usize, FxBatch>
        for UsersCBuild
    {
        fn new() -> FxResult<Self> {
            let schema = NullableOptions::indexed_true([2]).gen_schema(
                ["id", "name", "check"],
                [DataType::Int32, DataType::Utf8, DataType::Boolean],
            )?;
            let result = FxBatches::empty_with_schema(schema);
            let buffer = Some(Users::gen_batch_builder());

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

        fn mut_result(&mut self) -> &mut FxBatches<FxBatch> {
            &mut self.result
        }

        fn take_result(self) -> FxBatches<FxBatch> {
            self.result
        }
    }

    // 6. impl builder generator for the schema struct
    impl FxBatchBatchesBuilderGenerator for Users {
        type BatchBuilder = UsersEBuild;

        type BatchesBuilder = UsersCBuild;
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

        let mut bd = Users::gen_batch_batches_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}

// This case is actually the default derived macro's behavior
#[cfg(test)]
mod multiple_schemed_builder {

    use std::marker::PhantomData;

    use fx::datatypes::DataType;

    use super::*;

    // This part is the same as `test_builder`'s first part.
    #[derive(Clone)]
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
    impl FxEclecticBuilder<Users, ChunkArr> for UsersEBuild<ChunkArr> {
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

    impl FxChunkBuilderGenerator for Users {
        type ChunkBuilder = UsersEBuild<ChunkArr>;
    }

    // the second impl: FxBatch
    impl FxEclecticBuilder<Users, FxBatch> for UsersEBuild<FxBatch> {
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

            FxBatch::try_new_with_names(vec![c1, c2, c3], ["id", "name", "check"])
        }
    }

    impl FxBatchBuilderGenerator for Users {
        type BatchBuilder = UsersEBuild<FxBatch>;
    }

    // the third impl: [ArcArr; 3]
    impl FxEclecticBuilder<Users, [ArcArr; 3]> for UsersEBuild<[ArcArr; 3]> {
        fn new() -> Self {
            Self {
                id: Vec::new(),
                name: Vec::new(),
                check: Vec::new(),
                _e: PhantomData,
            }
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<[ArcArr; 3]> {
            let c1 = ArcArr::from_vec(self.id);
            let c2 = ArcArr::from_vec(self.name);
            let c3 = ArcArr::from_vec(self.check);

            Ok([c1, c2, c3])
        }
    }

    impl FxArraaBuilderGenerator<3> for Users {
        type ArraaBuilder = UsersEBuild<[ArcArr; 3]>;
    }

    // the fourth impl: FxBundle<3, ArcArr>
    impl FxEclecticBuilder<Users, FxBundle<3, ArcArr>> for UsersEBuild<FxBundle<3, ArcArr>> {
        fn new() -> Self {
            Self {
                id: Vec::new(),
                name: Vec::new(),
                check: Vec::new(),
                _e: PhantomData,
            }
        }

        fn stack(&mut self, row: Users) -> &mut Self {
            self.id.push(row.id);
            self.name.push(row.name);
            self.check.push(row.check);

            self
        }

        fn build(self) -> FxResult<FxBundle<3, ArcArr>> {
            let c1 = ArcArr::from_vec(self.id);
            let c2 = ArcArr::from_vec(self.name);
            let c3 = ArcArr::from_vec(self.check);

            Ok(FxBundle::new_with_names(
                [c1, c2, c3],
                ["id", "name", "check"],
            ))
        }
    }

    impl FxBundleBuilderGenerator<3> for Users {
        type BundleBuilder = UsersEBuild<FxBundle<3, ArcArr>>;
    }

    #[test]
    fn multiple_builders() {
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

        let mut arraa_builder = Users::gen_arraa_builder();
        let mut chunk_builder = Users::gen_chunk_builder();
        let mut batch_builder = Users::gen_batch_builder();
        let mut bundle_builder = Users::gen_bundle_builder();

        arraa_builder.stack(r1.clone()).stack(r2.clone());
        let arraa = arraa_builder.build();
        assert!(arraa.is_ok());
        println!("{:?}", arraa.unwrap());

        chunk_builder.stack(r1.clone()).stack(r2.clone());
        let chunk = chunk_builder.build();
        assert!(chunk.is_ok());
        println!("{:?}", chunk.unwrap());

        batch_builder.stack(r1.clone()).stack(r2.clone());
        let batch = batch_builder.build();
        assert!(batch.is_ok());
        println!("{:?}", batch.unwrap());

        bundle_builder.stack(r1).stack(r2);
        let bundle = bundle_builder.build();
        assert!(bundle.is_ok());
        println!("{:?}", bundle.unwrap());
    }

    #[derive(Debug)]
    struct UsersCSBuild {
        result: FxBatches<ChunkArr>,
        buffer: Option<UsersEBuild<ChunkArr>>,
    }

    impl
        FxCollectionBuilder<
            true,
            UsersEBuild<ChunkArr>,
            Users,
            FxBatches<ChunkArr>,
            usize,
            ChunkArr,
        > for UsersCSBuild
    {
        fn new() -> FxResult<Self> {
            let schema = NullableOptions::indexed_true([2]).gen_schema(
                ["id", "name", "check"],
                [DataType::Int32, DataType::Utf8, DataType::Boolean],
            )?;
            let result = FxBatches::<ChunkArr>::empty_with_schema(schema);
            let buffer = Some(Users::gen_chunk_builder());

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

    impl FxChunkBatchesBuilderGenerator for Users {
        type ChunkBuilder = UsersEBuild<ChunkArr>;

        type BatchesBuilder = UsersCSBuild;
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

        let mut bd = Users::gen_chunk_batches_builder().unwrap();

        bd.stack(r1).save().unwrap().stack(r2).save().unwrap();

        let d = bd.build();

        println!("{d:?}");
    }
}
