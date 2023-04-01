//! file: tabular.rs
//! author: Jacob Xie
//! date: 2023/04/01 13:23:41 Saturday
//! brief:

use std::ops::RangeBounds;

use arrow2::datatypes::{DataType, Field, Schema};
use inherent::inherent;

use super::{ArcArr, DequeArr, DequeIter, DequeIterMut};
use crate::ab::{private, Confined, Eclectic, FxSeq, Purport, StaticPurport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxTabular
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxTabular {
    schema: Schema,
    data: Vec<DequeArr>,
}

// ================================================================================================
// impl Confined for Vec<DequeArr>
//
// used for Receptacle
// ================================================================================================

impl Confined for Vec<DequeArr> {
    fn width(&self) -> usize {
        self.len()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.iter()
            .map(|dq| dq.datatype().unwrap_or(&DataType::Null))
            .collect()
    }
}

// ================================================================================================
// impl Purport
// ================================================================================================

impl StaticPurport for FxTabular {}

#[inherent]
impl Purport for FxTabular {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// Table methods
// ================================================================================================

fn from_vecaa(vecaa: Vec<ArcArr>) -> Vec<DequeArr> {
    vecaa.into_iter().map(DequeArr::from).collect()
}

impl FxTabular {
    // ============================================================================================
    // private methods
    // ============================================================================================

    fn _eclectic_into<E: Eclectic>(data: E) -> FxResult<Vec<ArcArr>> {
        let res = data
            .into_sequences()
            .into_iter()
            .map(|s| s.to_arc_array())
            .collect::<FxResult<Vec<_>>>()?;

        Ok(res)
    }

    fn _new<E, I, T>(data: E, names: Option<I>) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic,
    {
        let schema = match names {
            Some(n) => Self::gen_schema_with_names(&data, n),
            None => Self::gen_schema(&data),
        };

        let data = from_vecaa(Self::_eclectic_into(data)?);

        Ok(Self { schema, data })
    }

    fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::new(),
        }
    }

    // ============================================================================================
    // public methods
    // ============================================================================================

    /// Creates a new [`FxTabular`].
    /// # Panics
    /// Panics if data length mismatch.
    pub fn new<E: Eclectic>(data: E) -> Self {
        Self::try_new(data).unwrap()
    }

    /// Creates a new [`FxTabular`].
    /// # Errors
    /// This function will return an error if data length mismatch.
    pub fn try_new<E: Eclectic>(data: E) -> FxResult<Self> {
        Self::_new(data, Option::<&[&str]>::None)
    }

    /// Creates a new [`FxTabular`].
    /// # Panics
    /// Panics if data length mismatch.
    pub fn new_with_names<E, I, T>(data: E, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic,
    {
        Self::try_new_with_names(data, names).unwrap()
    }

    /// Creates a new [`FxTabular`].
    /// # Errors
    /// This function will return an error if data length mismatch.
    pub fn try_new_with_names<E, I, T>(data: E, names: I) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic,
    {
        Self::_new(data, Some(names))
    }

    /// Creates an empty [`FxTabular`].
    /// # Panics
    /// Panics if schema length mismatch.
    pub fn empty_with_schema(schema: Schema) -> Self {
        Self::try_empty_with_schema(schema).unwrap()
    }

    /// Creates an empty [`FxTabular`].
    /// # Errors
    /// This function will return an error if schema length mismatch.
    pub fn try_empty_with_schema(schema: Schema) -> FxResult<Self> {
        let sch = schema.clone();
        let data = schema
            .fields
            .into_iter()
            .map(|f| DequeArr::new_empty_with_type(f.data_type))
            .collect();

        Ok(Self { schema: sch, data })
    }

    /// Returns a reference to the data of this [`FxTabular`].
    pub fn data(&self) -> &[DequeArr] {
        &self.data
    }

    /// Returns the deque lens of this [`FxTabular`].
    pub fn deque_lens(&self) -> Vec<usize> {
        self.data.iter().map(|dq| dq.len()).collect()
    }

    /// Returns the array lens of this [`FxTabular`].
    pub fn array_lens(&self) -> Vec<usize> {
        self.data.iter().map(|dq| dq.array_len()).collect()
    }

    /// Returns the max deque len of this [`FxTabular`].
    pub fn max_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().max().cloned()
    }

    /// Returns the max array len of this [`FxTabular`].
    pub fn max_array_len(&self) -> Option<usize> {
        self.array_lens().iter().max().cloned()
    }

    /// Returns the min deque len of this [`FxTabular`].
    pub fn min_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().min().cloned()
    }

    /// Returns the min array len of this [`FxTabular`].
    pub fn min_array_len(&self) -> Option<usize> {
        self.array_lens().iter().min().cloned()
    }

    /// True if the deque lens are the same.
    pub fn is_deque_lens_equal(&self) -> bool {
        let l = self.deque_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    /// True if the array lens are the same.
    pub fn is_array_lens_equal(&self) -> bool {
        let l = self.array_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    /// True if all deques are empty.
    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|dq| dq.is_empty())
    }

    /// True if all deques has no datatype initialized.
    pub fn has_type(&self) -> bool {
        self.data.iter().all(|dq| dq.has_type())
    }

    /// Turns this [`FxTabular`] into a array of [`ArcArr`] vectors.
    pub fn into_arrays(self) -> Vec<Vec<ArcArr>> {
        self.data.into_iter().map(|dq| dq.into_arrays()).collect()
    }

    /// True if datatypes equals to self.datatypes.
    pub fn data_types_match(&self, datatypes: &[DataType]) -> bool {
        if datatypes.len() != self.width() {
            return false;
        }

        self.data
            .iter()
            .zip(datatypes.iter())
            .all(|(dq, d)| dq.data_type_match(d))
    }

    /// Returns references to each deque.
    pub fn as_slices(&self) -> Vec<(&[ArcArr], &[ArcArr])> {
        self.data.iter().map(|dq| dq.as_slices()).collect()
    }

    /// Makes all deque contiguos and returns their mutable references.
    pub fn make_contiguous(&mut self) -> Vec<&mut [ArcArr]> {
        self.data
            .iter_mut()
            .map(|dq| dq.make_contiguous())
            .collect()
    }

    /// Makes all deque contiguous and returns their references.
    pub fn make_as_slice(&mut self) -> Vec<&[ArcArr]> {
        self.make_contiguous();

        self.as_slices().iter().map(|s| s.0).collect()
    }

    pub fn deque_get(&self, index: usize) -> Vec<Option<&ArcArr>> {
        self.data.iter().map(|dq| dq.get(index)).collect()
    }

    pub fn deque_get_ok(&self, index: usize) -> FxResult<Vec<&ArcArr>> {
        let min_len = self.min_deque_len();

        if min_len.is_none() {
            return Err(FxError::EmptyContent);
        }

        if index > min_len.unwrap() {
            return Err(FxError::OutBounds);
        }

        Ok(self.data.iter().map(|dq| dq.get(index).unwrap()).collect())
    }

    pub fn deque_get_mut(&mut self, index: usize) -> Vec<Option<&mut ArcArr>> {
        self.data.iter_mut().map(|dq| dq.get_mut(index)).collect()
    }

    pub fn deque_get_mut_ok(&mut self, index: usize) -> FxResult<Vec<&mut ArcArr>> {
        let min_len = self.min_deque_len();

        if min_len.is_none() {
            return Err(FxError::EmptyContent);
        }

        if index > min_len.unwrap() {
            return Err(FxError::OutBounds);
        }

        Ok(self
            .data
            .iter_mut()
            .map(|dq| dq.get_mut(index).unwrap())
            .collect())
    }

    pub fn deque_insert<E: Eclectic>(&mut self, index: usize, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.insert(index, tmp)?;
        }

        Ok(())
    }

    pub fn deque_back(&self) -> Vec<Option<&ArcArr>> {
        self.data.iter().map(|dq| dq.back()).collect()
    }

    pub fn deque_back_mut(&mut self) -> Vec<Option<&mut ArcArr>> {
        self.data.iter_mut().map(|dq| dq.back_mut()).collect()
    }

    pub fn deque_front(&self) -> Vec<Option<&ArcArr>> {
        self.data.iter().map(|dq| dq.front()).collect()
    }

    pub fn deque_front_mut(&mut self) -> Vec<Option<&mut ArcArr>> {
        self.data.iter_mut().map(|dq| dq.front_mut()).collect()
    }

    pub fn deque_pop_back(&mut self) -> Vec<Option<ArcArr>> {
        self.data.iter_mut().map(|dq| dq.pop_back()).collect()
    }

    pub fn deque_pop_front(&mut self) -> Vec<Option<ArcArr>> {
        self.data.iter_mut().map(|dq| dq.pop_front()).collect()
    }

    pub fn deque_push_back<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.push_back(tmp)?;
        }

        Ok(())
    }

    pub fn deque_push_front<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.push_front(tmp)?;
        }

        Ok(())
    }

    pub fn deque_remove(&mut self, index: usize) -> Vec<Option<ArcArr>> {
        self.data.iter_mut().map(|dq| dq.remove(index)).collect()
    }

    pub fn deque_truncate(&mut self, len: usize) {
        self.data.iter_mut().for_each(|dq| dq.truncate(len));
    }

    pub fn deque_range<R>(&self, range: R) -> Vec<DequeIter<ArcArr>>
    where
        R: RangeBounds<usize> + Clone,
    {
        self.data.iter().map(|dq| dq.range(range.clone())).collect()
    }

    pub fn deque_range_mut<R>(&mut self, range: R) -> Vec<DequeIterMut<ArcArr>>
    where
        R: RangeBounds<usize> + Clone,
    {
        self.data
            .iter_mut()
            .map(|dq| dq.range_mut(range.clone()))
            .collect()
    }

    pub fn deque_iter(&self) -> Vec<DequeIter<ArcArr>> {
        self.data.iter().map(|dq| dq.iter()).collect()
    }

    pub fn deque_iter_mut(&mut self) -> Vec<DequeIterMut<ArcArr>> {
        self.data.iter_mut().map(|dq| dq.iter_mut()).collect()
    }

    // ================================================================================================
    // Functions with different name
    // ================================================================================================

    pub fn pop_back(&mut self) -> Vec<Option<ArcArr>> {
        self.deque_pop_back()
    }

    pub fn pop_front(&mut self) -> Vec<Option<ArcArr>> {
        self.deque_pop_front()
    }

    pub fn push_back<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        self.deque_push_back(value)
    }

    pub fn push_front<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        self.deque_push_front(value)
    }
}

// ================================================================================================
// impl Receptacle
// ================================================================================================

// E -> FxTabular
impl<E> private::InnerReceptacle<true, usize, E> for FxTabular
where
    E: Eclectic + Confined,
{
    type OutRef<'a> = Vec<&'a ArcArr> where Self: 'a;
    type OutMut<'a> = Vec<&'a mut ArcArr> where Self : 'a;

    fn new_empty() -> Self {
        Self::new_empty()
    }

    fn ref_schema(&self) -> Option<&Schema> {
        Some(&self.schema)
    }

    fn get_chunk(&self, key: usize) -> FxResult<Self::OutRef<'_>> {
        self.deque_get_ok(key)
    }

    fn get_mut_chunk(&mut self, key: usize) -> FxResult<Self::OutMut<'_>> {
        self.deque_get_mut_ok(key)
    }

    fn insert_chunk_type_unchecked(&mut self, key: usize, data: E) -> FxResult<()> {
        self.deque_insert(key, data)
    }

    fn remove_chunk(&mut self, key: usize) -> FxResult<()> {
        self.deque_remove(key);

        Ok(())
    }

    fn push_chunk_type_unchecked(&mut self, data: E) -> FxResult<()> {
        self.deque_push_back(data)
    }

    fn pop_chunk(&mut self) -> FxResult<()> {
        self.deque_pop_back();

        Ok(())
    }
}
