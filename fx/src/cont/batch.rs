//! file: rbatch.rs
//! author: Jacob Xie
//! date: 2023/01/20 12:36:42 Friday
//! brief: Batch

use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use inherent::inherent;

use crate::ab::{private, FxSeq, Purport, StaticPurport};
use crate::cont::ArcArr;
use crate::error::FxResult;

use super::ChunkArr;

// ================================================================================================
// FxBatch
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxBatch {
    pub(crate) schema: Schema,
    pub(crate) data: ChunkArr,
}

// ================================================================================================
// impl Purport
// ================================================================================================

#[inherent]
impl Purport for FxBatch {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// impl Eclectic & EclecticMut
// ================================================================================================

impl private::InnerEclectic for FxBatch {
    type Seq = ArcArr;

    fn from_vec_seq(data: Vec<Self::Seq>) -> FxResult<Self>
    where
        Self: Sized,
    {
        Self::try_new(data)
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.data.arrays()
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        self.data = Chunk::try_new(arrays)?;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.data.into_arrays()
    }
}

impl private::InnerEclecticMutChunk for FxBatch {
    fn mut_chunk(&mut self) -> &mut ChunkArr {
        &mut self.data
    }
}

// ================================================================================================
// Batch methods
// ================================================================================================

impl FxBatch {
    // watch out panic if data's length are not the same
    pub fn new(data: Vec<ArcArr>) -> Self {
        FxBatch::try_new(data).expect("data length should always be the same")
    }

    pub fn try_new(data: Vec<ArcArr>) -> FxResult<Self> {
        Ok(Self {
            schema: Self::gen_schema(&data),
            data: Chunk::try_new(data)?,
        })
    }

    pub fn new_with_names<I, T>(data: Vec<ArcArr>, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        FxBatch::try_new_with_names(data, names).expect("data length should always be the same")
    }

    pub fn try_new_with_names<I, T>(data: Vec<ArcArr>, names: I) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        Ok(Self {
            schema: Self::gen_schema_with_names(&data, names),
            data: Chunk::try_new(data)?,
        })
    }

    pub fn empty_with_schema(schema: Schema) -> Self {
        let arrays = schema
            .fields
            .iter()
            .map(|f| ArcArr::new_empty(f.data_type.clone()))
            .collect::<Vec<_>>();
        let data = ChunkArr::new(arrays);
        Self { schema, data }
    }

    pub fn data(&self) -> &ChunkArr {
        &self.data
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_batch {
    use super::*;
    use crate::ab::*;

    #[test]
    fn new_fx_batch_should_be_successful() {
        let arrays = vec![
            ArcArr::from_slice(["a", "c", "x"]),
            ArcArr::from_slice([Some("x"), None, Some("y")]),
            ArcArr::from_slice([true, false, false]),
        ];

        let batch = FxBatch::try_new(arrays);

        assert!(batch.is_ok());

        let batch = batch.unwrap();
        println!("{batch:?}");
        println!("{:?}", batch.is_lens_same());
    }

    #[test]
    fn extend_should_be_successful() {
        let arrays = vec![
            ArcArr::from_slice(["a", "c", "x"]),
            ArcArr::from_slice([Some("x"), None, Some("y")]),
            ArcArr::from_slice([true, false, false]),
        ];

        let mut batch1 = FxBatch::new(arrays);

        let arrays = vec![
            ArcArr::from_slice(["a", "c", "x"]),
            ArcArr::from_slice([Some("x"), None, Some("y")]),
            ArcArr::from_slice([true, false, false]),
        ];

        let batch2 = FxBatch::new(arrays);

        let ext_res = batch1.try_extent(&batch2);
        assert!(ext_res.is_ok());

        println!("{batch1:?}");
    }
}
