//! file: tabular.rs
//! author: Jacob Xie
//! date: 2023/04/01 13:23:41 Saturday
//! brief:

use std::ops::RangeBounds;

use arrow2::datatypes::{DataType, Field, Schema};
use inherent::inherent;

use super::{ArcArr, DequeArcArr, DequeIterMut, DequeIterOwned, DequeIterRef};
use crate::ab::dqs::{Dqs, EclecticGetMut};
use crate::ab::{private, Confined, Eclectic, FxSeq, Purport, StaticPurport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// FxTabular
// ================================================================================================

#[derive(Debug, Clone)]
pub struct FxTabular {
    schema: Schema,
    data: Vec<DequeArcArr>,
}

// ================================================================================================
// impl Confined for Vec<DequeArr>
//
// used for Receptacle
// ================================================================================================

impl Confined for Vec<DequeArcArr> {
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

fn from_vecaa(vecaa: Vec<ArcArr>) -> Vec<DequeArcArr> {
    vecaa.into_iter().map(DequeArcArr::from).collect()
}

// ================================================================================================
// impl Deqc
// ================================================================================================

impl EclecticGetMut for Vec<ArcArr> {
    fn _get_mut(&mut self, index: usize) -> Option<&mut ArcArr> {
        self.get_mut(index)
    }
}

#[inherent]
impl Dqs for FxTabular {
    type Data = Vec<DequeArcArr>;
    type RefData<'a> = &'a [DequeArcArr];
    type MutData<'a> = &'a mut [DequeArcArr];

    type EclecticInto = Vec<ArcArr>;
    type MakeContiguous<'a> = Vec<&'a mut [ArcArr]>;
    type MakeAsSlice<'a> = Vec<&'a [ArcArr]>;

    type Deques = Vec<ArcArr>;
    type RefDeques<'a> = Vec<&'a ArcArr>;
    type MutDeques<'a> = Vec<&'a mut ArcArr>;

    type OptDeques = Vec<Option<ArcArr>>;
    type OptRefDeques<'a> = Vec<Option<&'a ArcArr>>;
    type OptMutDeques<'a> = Vec<Option<&'a mut ArcArr>>;

    type DequesIter = Vec<DequeIterOwned<ArcArr>>;
    type DequesIterRef<'a> = Vec<DequeIterRef<'a, ArcArr>>;
    type DequesIterMut<'a> = Vec<DequeIterMut<'a, ArcArr>>;

    // ================================================================================================
    // private impl
    // ================================================================================================

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

    // ================================================================================================
    // public impl
    // ================================================================================================

    pub fn new_empty() -> Self {
        Self {
            schema: Schema::from(Vec::<Field>::new()),
            data: Vec::new(),
        }
    }

    pub fn try_empty_with_schema(schema: Schema) -> FxResult<Self> {
        let sch = schema.clone();
        let data = schema
            .fields
            .into_iter()
            .map(|f| DequeArcArr::new_empty_with_type(f.data_type))
            .collect();

        Ok(Self { schema: sch, data })
    }

    pub fn take_data(self) -> Vec<DequeArcArr> {
        self.data
    }

    pub fn ref_data(&self) -> &[DequeArcArr] {
        &self.data
    }

    pub fn mut_data(&mut self) -> &mut [DequeArcArr] {
        &mut self.data
    }

    // ================================================================================================
    // deque
    // ================================================================================================

    /// Makes all deque contiguos and returns their mutable references.
    pub fn make_contiguous(&mut self) -> Vec<&mut [ArcArr]> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.make_contiguous())
            .collect()
    }

    /// Makes all deque contiguous and returns their references.
    pub fn make_as_slice(&mut self) -> Vec<&[ArcArr]> {
        self.make_contiguous();

        self.data.iter().map(|s| s.as_slices().0).collect()
    }

    pub fn deque_get(&self, index: usize) -> Vec<Option<&ArcArr>> {
        self.ref_data()
            .into_iter()
            .map(|dq| dq.get(index))
            .collect()
    }

    pub fn deque_get_ok(&self, index: usize) -> FxResult<Vec<&ArcArr>> {
        let min_len = self.min_deque_len();

        if min_len.is_none() {
            return Err(FxError::EmptyContent);
        }

        if index > min_len.unwrap() {
            return Err(FxError::OutBounds);
        }

        Ok(self
            .ref_data()
            .into_iter()
            .map(|dq| dq.get(index).unwrap())
            .collect())
    }

    pub fn deque_get_mut(&mut self, index: usize) -> Vec<Option<&mut ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.get_mut(index))
            .collect()
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
            .mut_data()
            .into_iter()
            .map(|dq| dq.get_mut(index).unwrap())
            .collect())
    }

    pub fn deque_insert<E: Eclectic>(&mut self, index: usize, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.mut_data().into_iter().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value._get_mut(idx).unwrap());
            dq.insert(index, tmp)?;
        }

        Ok(())
    }

    pub fn deque_back(&self) -> Vec<Option<&ArcArr>> {
        self.ref_data().into_iter().map(|dq| dq.back()).collect()
    }

    pub fn deque_back_mut(&mut self) -> Vec<Option<&mut ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.back_mut())
            .collect()
    }

    pub fn deque_front(&self) -> Vec<Option<&ArcArr>> {
        self.ref_data().into_iter().map(|dq| dq.front()).collect()
    }

    pub fn deque_front_mut(&mut self) -> Vec<Option<&mut ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.front_mut())
            .collect()
    }

    pub fn deque_pop_back(&mut self) -> Vec<Option<ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.pop_back())
            .collect()
    }

    pub fn deque_pop_front(&mut self) -> Vec<Option<ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.pop_front())
            .collect()
    }

    pub fn deque_push_back<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.mut_data().into_iter().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value._get_mut(idx).unwrap());
            dq.push_back(tmp)?;
        }

        Ok(())
    }

    pub fn deque_push_front<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        let mut value = Self::_eclectic_into(value)?;

        for (idx, dq) in self.mut_data().into_iter().enumerate() {
            let mut tmp = ArcArr::new_empty(DataType::Null);
            std::mem::swap(&mut tmp, value._get_mut(idx).unwrap());
            dq.push_front(tmp)?;
        }

        Ok(())
    }

    pub fn deque_remove(&mut self, index: usize) -> Vec<Option<ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.remove(index))
            .collect()
    }

    pub fn deque_truncate(&mut self, len: usize) {
        self.mut_data().into_iter().for_each(|dq| dq.truncate(len));
    }

    pub fn deque_range<R>(&self, range: R) -> Vec<DequeIterRef<ArcArr>>
    where
        R: RangeBounds<usize> + Clone,
    {
        self.ref_data()
            .into_iter()
            .map(|dq| dq.range(range.clone()))
            .collect()
    }

    pub fn deque_range_mut<R>(&mut self, range: R) -> Vec<DequeIterMut<ArcArr>>
    where
        R: RangeBounds<usize> + Clone,
    {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.range_mut(range.clone()))
            .collect()
    }

    pub fn deque_iter(&self) -> Vec<DequeIterRef<ArcArr>> {
        self.ref_data().into_iter().map(|dq| dq.iter()).collect()
    }

    pub fn deque_iter_mut(&mut self) -> Vec<DequeIterMut<ArcArr>> {
        self.mut_data()
            .into_iter()
            .map(|dq| dq.iter_mut())
            .collect()
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

#[cfg(test)]
mod test_tabular {
    use super::*;
    use crate::ab::FromSlice;
    use crate::arc_arr;

    #[test]
    fn deqc_trait_success() {
        let d = FxTabular::new(vec![arc_arr!([1, 2, 3]), arc_arr!(["a", "b", "c"])]);

        println!("{:?}", d.deque_lens());
    }
}
