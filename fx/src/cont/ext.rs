//! file: ext.rs
//! author: Jacob Xie
//! date: 2023/02/20 20:09:06 Monday
//! brief: Arrow extensions

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

// use arrow2::array::TryPush;
use arrow2::array::TryExtendFromSelf;
use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Schema};

use crate::ab::{private, FxSeq, StaticPurport};
use crate::cont::macros::{arr_to_vec, arr_to_vec_p, try_ext_from_slf};
use crate::types::*;
use crate::{FxError, FxResult};

// ================================================================================================
// Arc<Array>
// ================================================================================================

impl FxSeq for ArcArr {
    fn new_nulls(data_type: DataType, length: usize) -> Self {
        match data_type {
            DataType::Boolean => BA::new_null(data_type, length).arced(),
            DataType::Int8 => PAi8::new_null(data_type, length).arced(),
            DataType::Int16 => PAi16::new_null(data_type, length).arced(),
            DataType::Int32 => PAi32::new_null(data_type, length).arced(),
            DataType::Int64 => PAi64::new_null(data_type, length).arced(),
            DataType::UInt8 => PAu8::new_null(data_type, length).arced(),
            DataType::UInt16 => PAu16::new_null(data_type, length).arced(),
            DataType::UInt32 => PAu32::new_null(data_type, length).arced(),
            DataType::UInt64 => PAu64::new_null(data_type, length).arced(),
            DataType::Float32 => PAf32::new_null(data_type, length).arced(),
            DataType::Float64 => PAf64::new_null(data_type, length).arced(),
            DataType::Utf8 => UA::new_null(data_type, length).arced(),
            _ => unimplemented!(),
        }
    }

    fn is_arr() -> bool {
        true
    }

    fn is_vec() -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_any_mut())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity().as_ref().map(|bm| bm.iter().collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }

    fn to_array(self) -> FxResult<ArcArr> {
        Ok(self)
    }

    fn to_vector(self) -> FxResult<ArcVec> {
        match &self.data_type() {
            DataType::Boolean => arr_to_vec!(self, BA, MB),
            DataType::Int8 => arr_to_vec_p!(self, PAi8, MPAi8),
            DataType::Int16 => arr_to_vec_p!(self, PAi16, MPAi16),
            DataType::Int32 => arr_to_vec_p!(self, PAi32, MPAi32),
            DataType::Int64 => arr_to_vec_p!(self, PAi64, MPAi64),
            DataType::UInt8 => arr_to_vec_p!(self, PAu8, MPAu8),
            DataType::UInt16 => arr_to_vec_p!(self, PAu16, MPAu16),
            DataType::UInt32 => arr_to_vec_p!(self, PAu32, MPAu32),
            DataType::UInt64 => arr_to_vec_p!(self, PAu64, MPAu64),
            DataType::Float32 => arr_to_vec_p!(self, PAf32, MPAf32),
            DataType::Float64 => arr_to_vec_p!(self, PAf64, MPAf64),
            DataType::Utf8 => arr_to_vec!(self, UA, MU),
            _ => Err(FxError::FailedToConvert),
        }
    }

    fn extend(&mut self, s: &ArcArr) -> FxResult<&mut Self> {
        let ct = concatenate(&[self.as_ref(), s.deref()])?;
        *self = Arc::from(ct);

        Ok(self)
    }
}

// ================================================================================================
// Arc<MutableArray>
// ================================================================================================

impl FxSeq for ArcVec {
    fn new_nulls(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::Boolean => Arc::new(MB::from(vec![None; len])),
            DataType::Int8 => Arc::new(MPAi8::from(vec![None; len])),
            DataType::Int16 => Arc::new(MPAi16::from(vec![None; len])),
            DataType::Int32 => Arc::new(MPAi32::from(vec![None; len])),
            DataType::Int64 => Arc::new(MPAi64::from(vec![None; len])),
            DataType::UInt8 => Arc::new(MPAu8::from(vec![None; len])),
            DataType::UInt16 => Arc::new(MPAu16::from(vec![None; len])),
            DataType::UInt32 => Arc::new(MPAu32::from(vec![None; len])),
            DataType::UInt64 => Arc::new(MPAu64::from(vec![None; len])),
            DataType::Float32 => Arc::new(MPAf32::from(vec![None; len])),
            DataType::Float64 => Arc::new(MPAf64::from(vec![None; len])),
            DataType::Utf8 => Arc::new(MU::from(vec![Option::<&str>::None; len])),
            _ => unimplemented!(),
        }
    }

    fn is_arr() -> bool {
        false
    }

    fn is_vec() -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_mut_any())
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (**self).data_type()
    }

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity().as_ref().map(|bm| bm.iter().collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }

    fn to_array(mut self) -> FxResult<ArcArr> {
        let res = Arc::get_mut(&mut self)
            .ok_or(FxError::FailedToConvert)?
            .as_arc();

        Ok(res)
    }

    fn to_vector(self) -> FxResult<ArcVec> {
        Ok(self)
    }

    fn extend(&mut self, s: &Self) -> FxResult<&mut Self> {
        match &self.data_type() {
            DataType::Boolean => try_ext_from_slf!(self, s, MB),
            DataType::Int8 => try_ext_from_slf!(self, s, MPAi8),
            DataType::Int16 => try_ext_from_slf!(self, s, MPAi16),
            DataType::Int32 => try_ext_from_slf!(self, s, MPAi32),
            DataType::Int64 => try_ext_from_slf!(self, s, MPAi64),
            DataType::UInt8 => try_ext_from_slf!(self, s, MPAu8),
            DataType::UInt16 => try_ext_from_slf!(self, s, MPAu16),
            DataType::UInt32 => try_ext_from_slf!(self, s, MPAu32),
            DataType::UInt64 => try_ext_from_slf!(self, s, MPAu64),
            DataType::Float32 => try_ext_from_slf!(self, s, MPAf32),
            DataType::Float64 => try_ext_from_slf!(self, s, MPAf64),
            DataType::Utf8 => try_ext_from_slf!(self, s, MU),
            _ => Err(FxError::FailedToConvert),
        }
    }
}

// ================================================================================================
// Default implementation for Chunk<dyn Array>
// ================================================================================================

impl StaticPurport for ChunkArr {}

impl private::InnerEclectic for ChunkArr {
    type Seq = ArcArr;

    fn empty() -> Self
    where
        Self: Sized,
    {
        Chunk::new(Vec::new())
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.arrays()
    }

    fn set_sequences_unchecked(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        Chunk::try_new(arrays)?;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.into_arrays()
    }
}

impl private::InnerEclecticMutChunk for Chunk<ArcArr> {
    fn mut_chunk(&mut self) -> &mut Chunk<ArcArr> {
        self
    }
}

// ================================================================================================
// Default implementation for Vec<Chunk<dyn Array>>
// ================================================================================================

impl StaticPurport for Vec<ChunkArr> {}

impl private::InnerEclecticCollection<false, usize, ChunkArr> for Vec<ChunkArr> {
    fn empty() -> Self
    where
        Self: Sized,
    {
        Vec::new()
    }

    fn ref_schema(&self) -> Option<&Schema> {
        None
    }

    fn ref_container(&self) -> Vec<&ChunkArr> {
        self.iter().collect()
    }

    fn get_chunk(&self, key: usize) -> FxResult<&ChunkArr> {
        self.get(key).ok_or(FxError::OutBounds)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<&mut ChunkArr> {
        self.get_mut(key).ok_or(FxError::OutBounds)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: ChunkArr) -> FxResult<()> {
        if key > self.len() {
            return Err(FxError::OutBounds);
        }

        self.insert(key, data);

        Ok(())
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        if key > self.len() {
            return Err(FxError::OutBounds);
        }

        self.remove(key);

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: ChunkArr) -> FxResult<()> {
        self.push(data);

        Ok(())
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.pop();

        Ok(())
    }

    fn take_container(self) -> Vec<ChunkArr> {
        self
    }
}

// ================================================================================================
// Default implementation for Map<I, Chunk<dyn Array>>
// ================================================================================================

impl<I> StaticPurport for HashMap<I, ChunkArr> where I: Hash + Eq {}

impl<IDX> private::InnerEclecticCollection<false, IDX, ChunkArr> for HashMap<IDX, ChunkArr>
where
    IDX: Hash + Eq,
{
    fn empty() -> Self
    where
        Self: Sized,
    {
        todo!()
    }

    fn ref_schema(&self) -> Option<&Schema> {
        todo!()
    }

    fn ref_container(&self) -> Vec<&ChunkArr> {
        self.iter().map(|(_, v)| v).collect()
    }

    fn get_chunk(&self, key: IDX) -> FxResult<&ChunkArr> {
        self.get(&key).ok_or(FxError::NoKey)
    }

    fn get_mut_chunk(&mut self, key: IDX) -> FxResult<&mut ChunkArr> {
        self.get_mut(&key).ok_or(FxError::NoKey)
    }

    fn insert_chunk_type_unchecked(&mut self, key: IDX, data: ChunkArr) -> FxResult<()> {
        self.insert(key, data).ok_or(FxError::NoKey)?;

        Ok(())
    }

    fn remove_chunk(&mut self, key: IDX) -> FxResult<()> {
        self.remove(&key).ok_or(FxError::NoKey)?;

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, _data: ChunkArr) -> FxResult<()> {
        todo!()
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        todo!()
    }

    fn take_container(self) -> Vec<ChunkArr> {
        self.into_values().collect()
    }
}
