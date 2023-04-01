//! file: constant.rs
//! author: Jacob Xie
//! date: 2023/03/15 20:36:00 Wednesday
//! brief:

// ================================================================================================
// Constant
// ================================================================================================

pub(crate) const CHUNK: &str = "chunk"; // Chunk<Arc<dyn Array>>
pub(crate) const ARRAA: &str = "arraa"; // [Arc<dyn Array>; W]. 'arraa' denotes (Array of ArcArr)
pub(crate) const BATCH: &str = "batch"; // FxBatch
pub(crate) const BUNDLE: &str = "bundle"; // FxBundle<W; Arc<dyn Array>>
pub(crate) const TABLE: &str = "table"; // FxTable
pub(crate) const TABULAR: &str = "tabular"; // FxTabular

pub(crate) const FX_OPTIONS: [&str; 5] = [CHUNK, BATCH, BUNDLE, TABLE, TABULAR];

// Note: Array is a trait provided by [arrow](https://github.com/jorgecarleitao/arrow2)
