//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder
//!
//! By deriving `Fx` proc-macro on a struct, builder traits are used in the sense of auto generating
//! `Eclectic` or `Receptacle`.
//! Please check [tests/fx_builder_test.rs] for manual implement.

use std::hash::Hash;

use crate::ab::{Confined, Eclectic, Receptacle};
use crate::cont::{ArcArr, ChunkArr, FxBatch, FxBatches, FxBundle, FxBundles, FxTable};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxEclecticBuilder
//
// 1. FxChunkBuilderGenerator:  -> ChunkArr
// 2. FxArrBuilderGenerator:    -> [ArcArr; W]
// 3. FxBatchBuilderGenerator:  -> FxBatch
// 4. FxBundleBuilderGenerator:  -> FxBundle<W, ArcArr>
//
// Based on a named struct, generate a new struct with several vector fields, and each of them
// denotes its original data type (`Option` is supported).
// And this process has been concluded in `fx-macros`, which used procedure macro to auto generate
// all the required implementations for a struct who represents a schema.
// ================================================================================================

pub trait FxEclecticBuilder<R, T>: Sized + Send
where
    T: Confined,
{
    fn new() -> Self;

    fn stack(&mut self, row: R) -> &mut Self;

    fn build(self) -> FxResult<T>;
}

// -> ChunkArr
pub trait FxChunkBuilderGenerator: Sized {
    type ChunkBuilder: FxEclecticBuilder<Self, ChunkArr>;

    fn gen_chunk_builder() -> Self::ChunkBuilder {
        Self::ChunkBuilder::new()
    }
}

// -> [ArcArr; W]
pub trait FxArraaBuilderGenerator<const W: usize>: Sized {
    type ArraaBuilder: FxEclecticBuilder<Self, [ArcArr; W]>;

    fn gen_arraa_builder() -> Self::ArraaBuilder {
        Self::ArraaBuilder::new()
    }
}

// -> FxBatch
pub trait FxBatchBuilderGenerator: Sized {
    type BatchBuilder: FxEclecticBuilder<Self, FxBatch>;

    fn gen_batch_builder() -> Self::BatchBuilder {
        Self::BatchBuilder::new()
    }
}

// -> FxBundle<W, ArcArr>
pub trait FxBundleBuilderGenerator<const W: usize>: Sized {
    type BundleBuilder: FxEclecticBuilder<Self, FxBundle<W, ArcArr>>;

    fn gen_bundle_builder() -> Self::BundleBuilder {
        Self::BundleBuilder::new()
    }
}

// ================================================================================================
// FxCollectionBuilder
//
// 1. FxChunksBuilderGenerator:         ChunkArr -> Vec<ChunkArr>
// 2. FxChunkBatchesBuilderGenerator:   ChunkArr -> Batches
// 3. FxBatchBatchesBuilderGenerator:   Batch -> Batches
// 4. FxBundleBatchesBuilderGenerator:  Bundle -> Batches
// 5. FxBundlesBuilderGenerator:        [ArcArr; W] -> Bundles
// 6. FxArraaTableGenerator:            [ArcArr; W] -> Table
// 7. FxChunkTableGenerator:            ChunkArr -> Table
// 8. FxBatchTableGenerator:            Batch -> Table
// ================================================================================================

pub trait FxCollectionBuilder<const SCHEMA: bool, B, R, T, I, C>: Sized + Send
where
    B: FxEclecticBuilder<R, C>,
    T: Receptacle<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic + Confined,
{
    fn new() -> FxResult<Self>;

    fn mut_buffer(&mut self) -> Option<&mut B>;

    fn set_buffer(&mut self, buffer: B);

    fn take_buffer(&mut self) -> Option<B>;

    fn mut_result(&mut self) -> &mut T;

    fn take_result(self) -> T;

    // default impl

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

// ChunkArr -> Vec<ChunkArr>
pub trait FxChunksBuilderGenerator: Sized {
    type ChunkBuilder: FxEclecticBuilder<Self, ChunkArr>;

    type ChunksBuilder: FxCollectionBuilder<
        false,
        Self::ChunkBuilder,
        Self,
        Vec<ChunkArr>,
        usize,
        ChunkArr,
    >;

    fn gen_chunks_builder() -> FxResult<Self::ChunksBuilder> {
        Self::ChunksBuilder::new()
    }
}

// ChunkArr -> Batches
pub trait FxChunkBatchesBuilderGenerator: Sized {
    type ChunkBuilder: FxEclecticBuilder<Self, ChunkArr>;

    type BatchesBuilder: FxCollectionBuilder<
        true,
        Self::ChunkBuilder,
        Self,
        FxBatches<ChunkArr>,
        usize,
        ChunkArr,
    >;

    fn gen_chunk_batches_builder() -> FxResult<Self::BatchesBuilder> {
        Self::BatchesBuilder::new()
    }
}

// Batch -> Batches
pub trait FxBatchBatchesBuilderGenerator: Sized {
    type BatchBuilder: FxEclecticBuilder<Self, FxBatch>;

    type BatchesBuilder: FxCollectionBuilder<
        true,
        Self::BatchBuilder,
        Self,
        FxBatches<FxBatch>,
        usize,
        FxBatch,
    >;

    fn gen_batch_batches_builder() -> FxResult<Self::BatchesBuilder> {
        Self::BatchesBuilder::new()
    }
}

// Bundle -> Batches
pub trait FxBundleBatchesBuilderGenerator: Sized {
    type BundleBuilder<const W: usize>: FxEclecticBuilder<Self, FxBundle<W, ArcArr>>;

    type BatchesBuilder<const W: usize>: FxCollectionBuilder<
        true,
        Self::BundleBuilder<W>,
        Self,
        FxBatches<FxBundle<W, ArcArr>>,
        usize,
        FxBundle<W, ArcArr>,
    >;

    fn gen_bundle_batches_builder<const W: usize>() -> FxResult<Self::BatchesBuilder<W>> {
        Self::BatchesBuilder::<W>::new()
    }
}

// [ArcArr; W] -> Bundles
pub trait FxBundlesBuilderGenerator<const W: usize>: Sized {
    type ArraaBuilder: FxEclecticBuilder<Self, [ArcArr; W]>;

    type BundlesBuilder: FxCollectionBuilder<
        true,
        Self::ArraaBuilder,
        Self,
        FxBundles<W, ArcArr>,
        usize,
        [ArcArr; W],
    >;

    fn gen_bundles_builder() -> FxResult<Self::BundlesBuilder> {
        Self::BundlesBuilder::new()
    }
}

// [ArcArr; W] -> Table
pub trait FxArraaTableGenerator<const W: usize>: Sized {
    type ArraaBuilder: FxEclecticBuilder<Self, [ArcArr; W]>;

    type TableBuilder: FxCollectionBuilder<
        true,
        Self::ArraaBuilder,
        Self,
        FxTable<W>,
        usize,
        [ArcArr; W],
    >;

    fn gen_arraa_table_builder() -> FxResult<Self::TableBuilder> {
        Self::TableBuilder::new()
    }
}

// ChunkArr -> Table
pub trait FxChunkTableGenerator<const W: usize>: Sized {
    type ChunkBuilder: FxEclecticBuilder<Self, ChunkArr>;

    type TableBuilder: FxCollectionBuilder<
        true,
        Self::ChunkBuilder,
        Self,
        FxTable<W>,
        usize,
        ChunkArr,
    >;

    fn gen_chunk_table_builder() -> FxResult<Self::TableBuilder> {
        Self::TableBuilder::new()
    }
}

// Batch -> Table
pub trait FxBatchTableGenerator<const W: usize>: Sized {
    type BatchBuilder: FxEclecticBuilder<Self, FxBatch>;

    type TableBuilder: FxCollectionBuilder<
        true,
        Self::BatchBuilder,
        Self,
        FxTable<W>,
        usize,
        FxBatch,
    >;

    fn gen_batch_table_builder() -> FxResult<Self::TableBuilder> {
        Self::TableBuilder::new()
    }
}

// ================================================================================================
// Test (Check tests/fx_builder_test.rs)
// ================================================================================================
