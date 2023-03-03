//! file: builder.rs
//! author: Jacob Xie
//! date: 2023/01/31 14:14:43 Tuesday
//! brief: Builder
//!
//! By deriving `Fx` proc-macro on a struct, builder traits are used in the sense of auto generating
//! `Eclectic` or `EclecticCollection`.
//! Please check [tests/fx_builder_test.rs] for manual implement.

use std::hash::Hash;

use crate::ab::{Eclectic, EclecticCollection};
use crate::cont::{ArcArr, ChunkArr, FxBatch, FxBatches, FxBundle, FxBundles};
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
    T: Eclectic,
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
// 4. FxBundleBatchesBuilderGenerator:   Table -> Batches
// 5. FxBundlesBuilderGenerator:         [ArcArr; W] -> Tables
// ================================================================================================

pub trait FxCollectionBuilder<const SCHEMA: bool, B, R, T, I, C>: Sized + Send
where
    B: FxEclecticBuilder<R, C>,
    T: EclecticCollection<SCHEMA, I, C>,
    I: Hash + Eq,
    C: Eclectic,
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

// Table -> Batches
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

    fn gen_table_batches_builder<const W: usize>() -> FxResult<Self::BatchesBuilder<W>> {
        Self::BatchesBuilder::<W>::new()
    }
}

// [ArcArr; W] -> Tables
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

    fn gen_tables_builder() -> FxResult<Self::BundlesBuilder> {
        Self::BundlesBuilder::new()
    }
}

// ================================================================================================
// Test (Check tests/fx_builder_test.rs)
// ================================================================================================
