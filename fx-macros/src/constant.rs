//! file: constant.rs
//! author: Jacob Xie
//! date: 2023/02/23 19:08:53 Thursday
//! brief: Constant

// ================================================================================================
// Constants
// ================================================================================================

// sequence type
#[allow(dead_code)]
pub(crate) const FX_ARC_ARR: &str = "ArcArr";
#[allow(dead_code)]
pub(crate) const FX_ARC_VEC: &str = "ArcVec";

// eclectic type
pub(crate) const FX_VEC_ARC_ARR: &str = "VecArcArr";
pub(crate) const FX_VEC_ARC_VEC: &str = "VecArcVec";
pub(crate) const FX_CHUNK_ARR: &str = "ChunkArr";
pub(crate) const FX_BATCH: &str = "FxBatch";
pub(crate) const FX_TABLE: &str = "FxTable";

// container type
pub(crate) const FX_VEC_CHUNK: &str = "VecChunk";
pub(crate) const FX_MAP_CHUNK: &str = "MapChunk";
pub(crate) const FX_BATCHES: &str = "FxBatches";
pub(crate) const FX_TABLES: &str = "FxTables";

// eclectic type list
pub(crate) const ECLECTIC_TYPES: &[&str] = &[
    FX_VEC_ARC_ARR,
    FX_VEC_ARC_VEC,
    FX_CHUNK_ARR,
    FX_BATCH,
    FX_TABLE,
];

// container type list
pub(crate) const CONTAINER_TYPES: &[&str] = &[
    //
    FX_VEC_CHUNK,
    FX_MAP_CHUNK,
    FX_BATCHES,
    FX_TABLES,
];

// ================================================================================================
// Fn
// ================================================================================================

pub(crate) fn get_eclectic_type(s: &str) -> Option<String> {
    if ECLECTIC_TYPES.contains(&s) {
        Some(s.to_string())
    } else {
        None
    }
}

pub(crate) fn get_container_type(s: &str) -> Option<String> {
    if CONTAINER_TYPES.contains(&s) {
        Some(s.to_string())
    } else {
        None
    }
}
