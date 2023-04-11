//! file: table.rs
//! author: Jacob Xie
//! date: 2023/03/04 01:31:56 Saturday
//! brief:

use std::ops::RangeBounds;

use arrow2::datatypes::{DataType, Field, Schema};
use inherent::inherent;

use super::{ArcArr, DequeArcArr, DequeIterMut, DequeIterOwned, DequeIterRef};
use crate::ab::dqs::{Dqs, EclecticGetMut};
use crate::ab::{private, Confined, Eclectic, FxSeq, Purport, StaticPurport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxTable
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxTable<const W: usize> {
    schema: Schema,
    data: [DequeArcArr; W],
}

// ================================================================================================
// impl Confined for [DequeArr; W]
//
// used for Receptacle
// ================================================================================================

impl<const W: usize> Confined for [DequeArcArr; W] {
    fn width(&self) -> usize {
        W
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

fn from_arraa<const W: usize>(arraa: [ArcArr; W]) -> [DequeArcArr; W] {
    arraa.map(DequeArcArr::from)
}

// ================================================================================================
// impl Deqc
// ================================================================================================

impl<const W: usize> EclecticGetMut for [ArcArr; W] {
    fn _get_mut(&mut self, index: usize) -> Option<&mut ArcArr> {
        self.get_mut(index)
    }
}

#[inherent]
impl<const W: usize> Dqs for FxTable<W> {
    type Data = [DequeArcArr; W];
    type RefData<'a> = &'a [DequeArcArr; W];
    type MutData<'a> = &'a mut [DequeArcArr; W];
    type EclecticInto = [ArcArr; W];

    type MakeContiguous<'a> = [&'a mut [ArcArr]; W];
    type MakeAsSlice<'a> = [&'a [ArcArr]; W];

    type Deques = [ArcArr; W];
    type RefDeques<'a> = [&'a ArcArr; W];
    type MutDeques<'a> = [&'a mut ArcArr; W];

    type OptDeques = [Option<ArcArr>; W];
    type OptRefDeques<'a> = [Option<&'a ArcArr>; W];
    type OptMutDeques<'a> = [Option<&'a mut ArcArr>; W];

    type DequesIter = [DequeIterOwned<ArcArr>; W];
    type DequesIterRef<'a> = [DequeIterRef<'a, ArcArr>; W];
    type DequesIterMut<'a> = [DequeIterMut<'a, ArcArr>; W];

    // ================================================================================================
    // private impl
    // ================================================================================================

    fn _eclectic_into<E: Eclectic>(data: E) -> FxResult<[ArcArr; W]> {
        if data.width() != W {
            return Err(FxError::LengthMismatch(data.width(), W));
        }

        let res = data
            .into_sequences()
            .into_iter()
            .map(|s| s.to_arc_array())
            .collect::<FxResult<Vec<_>>>()?
            .try_into()
            .unwrap();

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

        let data = from_arraa(Self::_eclectic_into(data)?);

        Ok(Self { schema, data })
    }

    // ================================================================================================
    // public impl
    // ================================================================================================

    pub fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: [(); W].map(|_| DequeArcArr::new_empty()),
        }
    }

    pub fn try_empty_with_schema(schema: Schema) -> FxResult<Self> {
        if schema.fields.len() != W {
            return Err(FxError::LengthMismatch(schema.fields.len(), W));
        }
        let sch = schema.clone();

        let mut idx = 0;
        let data: [DequeArcArr; W] = [(); W].map(|_| {
            let deque_arr =
                DequeArcArr::new_empty_with_type(schema.fields[idx].data_type().clone());
            idx += 1;
            deque_arr
        });

        Ok(Self { schema: sch, data })
    }

    pub fn take_data(self) -> [DequeArcArr; W] {
        self.data
    }

    pub fn ref_data(&self) -> &[DequeArcArr; W] {
        &self.data
    }

    pub fn mut_data(&mut self) -> &mut [DequeArcArr; W] {
        &mut self.data
    }

    // ================================================================================================
    // deque
    // ================================================================================================

    /// Makes all deque contiguos and returns their mutable references.
    pub fn make_contiguous(&mut self) -> [&mut [ArcArr]; W] {
        self.data.each_mut().map(|dq| dq.make_contiguous())
    }

    /// Makes all deque contiguous and returns their references.
    pub fn make_as_slice(&mut self) -> [&[ArcArr]; W] {
        self.make_contiguous();

        self.data.each_ref().map(|dq| dq.as_slices().0)
    }

    pub fn deque_get(&self, index: usize) -> [Option<&ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.get(index))
    }

    pub fn deque_get_ok(&self, index: usize) -> FxResult<[&ArcArr; W]> {
        let min_len = self.min_deque_len();

        if min_len.is_none() {
            return Err(FxError::EmptyContent);
        }

        if index > min_len.unwrap() {
            return Err(FxError::OutBounds);
        }

        Ok(self.data.each_ref().map(|dq| dq.get(index).unwrap()))
    }

    pub fn deque_get_mut(&mut self, index: usize) -> [Option<&mut ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.get_mut(index))
    }

    pub fn deque_get_mut_ok(&mut self, index: usize) -> FxResult<[&mut ArcArr; W]> {
        let min_len = self.min_deque_len();

        if min_len.is_none() {
            return Err(FxError::EmptyContent);
        }

        if index > min_len.unwrap() {
            return Err(FxError::OutBounds);
        }

        Ok(self.data.each_mut().map(|dq| dq.get_mut(index).unwrap()))
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

    pub fn deque_remove(&mut self, index: usize) -> [Option<ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.remove(index))
    }

    pub fn deque_truncate(&mut self, len: usize) {
        self.data.each_mut().map(|dq| dq.truncate(len));
    }

    pub fn deque_range<R>(&self, range: R) -> [DequeIterRef<ArcArr>; W]
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

    pub fn deque_iter(&self) -> [DequeIterRef<ArcArr>; W] {
        self.data.each_ref().map(|dq| dq.iter())
    }

    pub fn deque_iter_mut(&mut self) -> [DequeIterMut<ArcArr>; W] {
        self.data.each_mut().map(|dq| dq.iter_mut())
    }
}

// ================================================================================================
// impl Receptacle
// ================================================================================================

// E -> FxTable
impl<const W: usize, E> private::InnerReceptacle<true, usize, E> for FxTable<W>
where
    E: Eclectic + Confined,
{
    type OutRef<'a> = [&'a ArcArr; W] where Self: 'a;
    type OutMut<'a> = [&'a mut ArcArr; W] where Self : 'a;

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

#[cfg(test)]
mod test_table {
    use super::*;
    use crate::ab::FromSlice;
    use crate::arc_arr;

    #[test]
    fn dqs_trait_success() {
        let d = FxTable::<2>::new(vec![arc_arr!([1, 2, 3]), arc_arr!(["a", "b", "c"])]);

        println!("{:?}", d.deque_lens());
    }

    #[test]
    fn dqs_trait_size_equally_success() {
        let mut d = FxTable::<2>::new(vec![
            arc_arr!([1, 2, 3, 4, 5, 6]),
            arc_arr!(["a", "b", "c"]),
        ]);

        let dd = [arc_arr!([2, 3]), arc_arr!(["d", "e", "f", "g"])];

        d.push_back(dd).unwrap();

        d.size_equally(2);

        println!("{:?}", d);
    }

    #[test]
    fn dqs_trait_size_by_sequence_success() {
        let mut d = FxTable::<2>::new(vec![
            arc_arr!([1, 2, 3, 4, 5, 6]),
            arc_arr!(["a", "b", "c"]),
        ]);

        let dd = [arc_arr!([2, 3]), arc_arr!(["d", "e", "f", "g"])];

        d.push_back(dd).unwrap();

        let s = vec![1, 2, 3];
        d.size_by_sequence(&s);

        println!("{:?}", d);
    }
}
