//! file: table.rs
//! author: Jacob Xie
//! date: 2023/03/04 01:31:56 Saturday
//! brief:

use std::ops::RangeBounds;

use arrow2::datatypes::{DataType, Field, Schema};
use inherent::inherent;

use super::{ArcArr, DequeArr, DequeIter, DequeIterMut};
use crate::ab::{FxSeq, Purport, StaticPurport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxTable
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxTable<const W: usize> {
    schema: Schema,
    data: [DequeArr; W],
}

// ================================================================================================
// impl Purport
// ================================================================================================

impl<const W: usize> StaticPurport for FxTable<W> {}

#[inherent]
impl<const W: usize> Purport for FxTable<W> {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ================================================================================================
// Table methods
// ================================================================================================

impl<const W: usize> FxTable<W> {
    pub fn new(data: [ArcArr; W]) -> Self {
        Self {
            schema: Self::gen_schema(&data),
            data: data.map(|d| DequeArr::new(vec![d])),
        }
    }

    pub fn new_with_names<I, T>(data: [ArcArr; W], names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        Self {
            schema: Self::gen_schema_with_names(&data, names),
            data: data.map(|d| DequeArr::new(vec![d])),
        }
    }

    pub fn try_empty_with_schema(schema: Schema) -> FxResult<Self> {
        if schema.fields.len() != W {
            return Err(FxError::LengthMismatch(schema.fields.len(), W));
        }
        let sch = schema.clone();

        let mut idx = 0;
        let data: [DequeArr; W] = [(); W].map(|_| {
            let deque_arr = DequeArr::new_empty_with_type(schema.fields[idx].data_type().clone());
            idx += 1;
            deque_arr
        });

        Ok(Self { schema: sch, data })
    }

    pub fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: [(); W].map(|_| DequeArr::new_empty()),
        }
    }

    pub fn data(&self) -> &[DequeArr; W] {
        &self.data
    }

    pub fn deque_lens(&self) -> [usize; W] {
        self.data.each_ref().map(|dq| dq.len())
    }

    pub fn array_lens(&self) -> [usize; W] {
        self.data.each_ref().map(|dq| dq.array_len())
    }

    pub fn max_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().max().cloned()
    }

    pub fn max_array_len(&self) -> Option<usize> {
        self.array_lens().iter().max().cloned()
    }

    pub fn min_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().min().cloned()
    }

    pub fn min_array_len(&self) -> Option<usize> {
        self.array_lens().iter().min().cloned()
    }

    pub fn is_deque_lens_same(&self) -> bool {
        let l = self.deque_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    pub fn is_array_lens_same(&self) -> bool {
        let l = self.array_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|dq| dq.is_empty())
    }

    pub fn has_type(&self) -> bool {
        self.data.iter().all(|dq| dq.has_type())
    }

    pub fn into_arrays(self) -> [Vec<ArcArr>; W] {
        self.data.map(|dq| dq.into_arrays())
    }

    pub fn data_types_match(&self, datatypes: &[DataType]) -> bool {
        if datatypes.len() < W {
            return false;
        }

        self.data
            .iter()
            .zip(datatypes.iter())
            .all(|(dq, d)| dq.data_type_match(d))
    }

    pub fn as_slices(&self) -> [(&[ArcArr], &[ArcArr]); W] {
        self.data.each_ref().map(|dq| dq.as_slices())
    }

    pub fn make_contiguous(&mut self) -> [&mut [ArcArr]; W] {
        self.data.each_mut().map(|dq| dq.make_contiguous())
    }

    pub fn make_as_slice(&mut self) -> [&[ArcArr]; W] {
        self.make_contiguous();

        self.as_slices().each_ref().map(|s| s.0)
    }

    pub fn deque_get(&self, index: usize) -> [Option<&ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.get(index))
    }

    pub fn deque_get_mut(&mut self, index: usize) -> [Option<&mut ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.get_mut(index))
    }

    pub fn deque_insert(&mut self, index: usize, mut value: [ArcArr; W]) -> FxResult<()> {
        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.insert(index, tmp)?;
        }

        Ok(())
    }

    pub fn deque_back(&self) -> [Option<&ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.back())
    }

    pub fn deque_back_mut(&mut self) -> [Option<&mut ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.back_mut())
    }

    pub fn deque_front(&self) -> [Option<&ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.front())
    }

    pub fn deque_front_mut(&mut self) -> [Option<&mut ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.front_mut())
    }

    pub fn deque_pop_back(&mut self) -> [Option<ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.pop_back())
    }

    pub fn deque_pop_front(&mut self) -> [Option<ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.pop_front())
    }

    pub fn deque_push_back(&mut self, mut value: [ArcArr; W]) -> FxResult<()> {
        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.push_back(tmp)?;
        }

        Ok(())
    }

    pub fn deque_push_front(&mut self, mut value: [ArcArr; W]) -> FxResult<()> {
        for (idx, dq) in self.data.iter_mut().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value.get_mut(idx).unwrap());
            dq.push_front(tmp)?;
        }

        Ok(())
    }

    pub fn deque_truncate(&mut self, len: usize) {
        self.data.each_mut().map(|dq| dq.truncate(len));
    }

    pub fn deque_range<R>(&self, range: R) -> [DequeIter<ArcArr>; W]
    where
        R: RangeBounds<usize> + Clone,
    {
        self.data.each_ref().map(|dq| dq.range(range.clone()))
    }

    pub fn deque_range_mut<R>(&mut self, range: R) -> [DequeIterMut<ArcArr>; W]
    where
        R: RangeBounds<usize> + Clone,
    {
        self.data.each_mut().map(|dq| dq.range_mut(range.clone()))
    }

    pub fn deque_iter(&self) -> [DequeIter<ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.iter())
    }

    pub fn deque_iter_mut(&mut self) -> [DequeIterMut<ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.iter_mut())
    }
}
