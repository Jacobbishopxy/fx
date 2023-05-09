//! file: dqs.rs
//! author: Jacob Xie
//! date: 2023/04/09 09:16:08 Sunday
//! brief:

use std::ops::RangeBounds;

use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema};

use crate::ab::{Confined, Eclectic, Purport};
use crate::cont::{
    ArcArr, DequeArcArr, DequeIterMut, DequeIterOwned, DequeIterRef, SameSizedResult,
    SequenceSizedResult,
};
use crate::error::FxResult;

// ================================================================================================
// Dqs
// ================================================================================================

pub trait EclecticGetMut {
    fn _get_mut(&mut self, index: usize) -> Option<&mut ArcArr>;
}

pub trait Dqs: Sized + Confined + Purport {
    type Data: IntoIterator<Item = DequeArcArr>;
    type RefData<'a>: IntoIterator<Item = &'a DequeArcArr>
    where
        Self: 'a;
    type MutData<'a>: IntoIterator<Item = &'a mut DequeArcArr>
    where
        Self: 'a;

    type EclecticInto: EclecticGetMut;
    type MakeContiguous<'a>: IntoIterator<Item = &'a mut [ArcArr]>
    where
        Self: 'a;
    type MakeAsSlice<'a>: IntoIterator<Item = &'a [ArcArr]>
    where
        Self: 'a;

    type Deques: IntoIterator<Item = ArcArr>;
    type RefDeques<'a>: IntoIterator<Item = &'a ArcArr>
    where
        Self: 'a;
    type MutDeques<'a>: IntoIterator<Item = &'a mut ArcArr>
    where
        Self: 'a;

    type OptDeques: IntoIterator<Item = Option<ArcArr>>;
    type OptRefDeques<'a>: IntoIterator<Item = Option<&'a ArcArr>>
    where
        Self: 'a;
    type OptMutDeques<'a>: IntoIterator<Item = Option<&'a mut ArcArr>>
    where
        Self: 'a;

    type DequesIter: IntoIterator<Item = DequeIterOwned<ArcArr>>;
    type DequesIterRef<'a>: IntoIterator<Item = DequeIterRef<'a, ArcArr>>
    where
        Self: 'a;
    type DequesIterMut<'a>: IntoIterator<Item = DequeIterMut<'a, ArcArr>>
    where
        Self: 'a;

    // ================================================================================================
    // private methods
    // ================================================================================================

    fn _eclectic_into<E: Eclectic>(data: E) -> FxResult<Self::EclecticInto>;

    /// Creates a new [`impl Dqs`] .
    /// # Panics
    /// Panics if data length mismatch.
    fn _new<E, I, T>(data: E, names: Option<I>) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic;

    // ================================================================================================
    // public methods
    // ================================================================================================

    /// Creates a new empty [`impl Dqs`] .
    fn new_empty() -> Self;

    /// Creates an empty [`impl Dqs`] .
    /// # Errors
    /// This function will return an error if schema length mismatch.
    fn try_empty_with_schema(schema: Schema) -> FxResult<Self>;

    /// Returns a ownership to the data of this [`impl Dqs`] .
    fn take_data(self) -> Self::Data;

    /// Returns a reference to the data of this [`impl Dqs`] .
    fn ref_data(&self) -> Self::RefData<'_>;

    /// Returns a mutable reference to the data of this [`impl Dqs`] .
    fn mut_data(&mut self) -> Self::MutData<'_>;

    /// Makes all deque contiguous
    fn make_contiguous(&mut self) -> Self::MakeContiguous<'_>;

    /// Makes all deque contiguous and returns their references.
    fn make_as_slice(&mut self) -> Self::MakeAsSlice<'_>;

    fn deque_get(&self, index: usize) -> Self::OptRefDeques<'_>;

    fn deque_get_ok(&self, index: usize) -> FxResult<Self::RefDeques<'_>>;

    fn deque_get_mut(&mut self, index: usize) -> Self::OptMutDeques<'_>;

    fn deque_get_mut_ok(&mut self, index: usize) -> FxResult<Self::MutDeques<'_>>;

    fn deque_insert<E: Eclectic>(&mut self, index: usize, value: E) -> FxResult<()>;

    fn deque_back(&self) -> Self::OptRefDeques<'_>;

    fn deque_back_mut(&mut self) -> Self::OptMutDeques<'_>;

    fn deque_front(&self) -> Self::OptRefDeques<'_>;

    fn deque_front_mut(&mut self) -> Self::OptMutDeques<'_>;

    fn deque_pop_back(&mut self) -> Self::OptDeques;

    fn deque_pop_front(&mut self) -> Self::OptDeques;

    fn deque_push_back<E: Eclectic>(&mut self, value: E) -> FxResult<()>;

    fn deque_push_front<E: Eclectic>(&mut self, value: E) -> FxResult<()>;

    fn deque_remove(&mut self, index: usize) -> Self::OptDeques;

    fn deque_truncate(&mut self, len: usize);

    fn deque_range<R>(&self, range: R) -> Self::DequesIterRef<'_>
    where
        R: RangeBounds<usize> + Clone;

    fn deque_range_mut<R>(&mut self, range: R) -> Self::DequesIterMut<'_>
    where
        R: RangeBounds<usize> + Clone;

    fn deque_iter(&self) -> Self::DequesIterRef<'_>;

    fn deque_iter_mut(&mut self) -> Self::DequesIterMut<'_>;

    // ================================================================================================
    // default methods
    // ================================================================================================

    /// Creates a new [`impl Dqs`] .
    /// # Panics
    /// Panics if data length mismatch.
    fn new<E: Eclectic>(data: E) -> Self {
        Self::try_new(data).unwrap()
    }

    /// Creates a new [`impl Dqs`] .
    /// # Errors
    /// This function will return an error if data length mismatch.
    fn try_new<E: Eclectic>(data: E) -> FxResult<Self> {
        Self::_new(data, Option::<&[&str]>::None)
    }

    /// Creates a new [`impl Dqs`] .
    /// # Panics
    /// Panics if data length mismatch.
    fn new_with_names<E, I, T>(data: E, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic,
    {
        Self::try_new_with_names(data, names).unwrap()
    }

    /// Creates a new [`impl Dqs`] .
    /// # Errors
    /// This function will return an error if data length mismatch.
    fn try_new_with_names<E, I, T>(data: E, names: I) -> FxResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
        E: Eclectic,
    {
        Self::_new(data, Some(names))
    }

    /// Creates an empty [`impl Dqs`] .
    /// # Panics
    /// Panics if schema length mismatch.
    fn empty_with_schema(schema: Schema) -> Self {
        Self::try_empty_with_schema(schema).unwrap()
    }

    /// Returns the deque lens of this [`impl Dqs`] .
    fn deque_lens(&self) -> Vec<usize> {
        self.ref_data()
            .into_iter()
            .map(|dq: &DequeArcArr| dq.len())
            .collect()
    }

    /// Returns the array lens of this [`impl Dqs`] .
    fn array_lens(&self) -> Vec<usize> {
        self.ref_data()
            .into_iter()
            .map(|dq: &DequeArcArr| dq.array_len())
            .collect()
    }

    /// Returns the max deque len of this [`impl Dqs`] .
    fn max_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().max().copied()
    }

    /// Returns the min deque len of this [`impl Dqs`] .
    fn min_deque_len(&self) -> Option<usize> {
        self.deque_lens().iter().min().copied()
    }

    /// Returns the max array len of this [`impl Dqs`] .
    fn max_array_len(&self) -> Option<usize> {
        self.array_lens().iter().max().copied()
    }

    /// Returns the min array len of this [`impl Dqs`] .
    fn min_array_len(&self) -> Option<usize> {
        self.array_lens().iter().min().copied()
    }

    /// True if the deque lens are the same.
    fn is_deque_lens_equal(&self) -> bool {
        let l = self.deque_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    /// True if the array lens are the same.
    fn is_array_lens_equal(&self) -> bool {
        let l = self.array_lens();

        l.first()
            .map(|first| l.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    /// True if all deques are empty.
    fn is_empty(&self) -> bool {
        self.ref_data()
            .into_iter()
            .all(|dq: &DequeArcArr| dq.is_empty())
    }

    /// True if all deques has no datatype initialized.
    fn has_type(&self) -> bool {
        self.ref_data()
            .into_iter()
            .all(|dq: &DequeArcArr| dq.has_type())
    }

    /// Turns this [`impl Dqs`]  into a array of [`ArcArr`] vectors.
    fn into_arrays(self) -> Vec<Vec<ArcArr>> {
        self.take_data()
            .into_iter()
            .map(|dq: DequeArcArr| dq.into_arrays())
            .collect::<Vec<_>>()
    }

    /// True if datatypes equals to self.datatypes.
    fn data_types_match(&self, datatypes: &[DataType]) -> bool {
        if datatypes.len() != self.width() {
            return false;
        }

        self.ref_data()
            .into_iter()
            .zip(datatypes.iter())
            .all(|(dq, d)| dq.data_type_match(d))
    }

    /// Sizing arrays in each deque into the same size.
    fn size_equally(&mut self, len: usize) -> Vec<SameSizedResult> {
        self.mut_data()
            .into_iter()
            .map(|dq: &mut DequeArcArr| dq.size_arrays_equally(len))
            .collect()
    }

    /// Sizing arrays in each deque equally and transform them into [`Vec<Chunk<ArcArr>>`]
    /// The rest data which cannot be transformed to [`chunk`] will still be left in `self.data`.
    fn to_chunks_equally(&mut self, len: usize) -> Vec<Chunk<ArcArr>> {
        self.size_equally(len);
        let mut res = Vec::new();

        'bk: loop {
            // if any of the dq does not match the `len`, break the loop
            for dq in self.ref_data().into_iter() {
                match dq.front() {
                    Some(a) if a.len() == len => { /* pass */ }
                    _ => break 'bk,
                }
            }

            let mut chunk_buf = Vec::new();
            for dq in self.mut_data().into_iter() {
                match dq.pop_front() {
                    Some(a) => chunk_buf.push(a),
                    None => break 'bk,
                }
            }

            let chunk = Chunk::new(chunk_buf);
            res.push(chunk);
        }

        res
    }

    /// Sizing arrays in each deque by size sequence.
    fn size_by_sequence<'a, I>(&mut self, sequence: &'a I) -> Vec<SequenceSizedResult>
    where
        &'a I: IntoIterator<Item = &'a usize>,
    {
        self.mut_data()
            .into_iter()
            .map(|dq: &mut DequeArcArr| dq.size_arrays_by_sequence(sequence))
            .collect()
    }

    /// Sizing arrays in each deque by size sequence and transform them into [`Vec<Chunk<ArcArr>>`]
    /// The rest data which cannot be transformed to [`chunk`] will still be left in `self.data`.
    fn to_chunks_by_sequence<'a, I>(&mut self, sequence: &'a I) -> Vec<Chunk<ArcArr>>
    where
        &'a I: IntoIterator<Item = &'a usize>,
    {
        self.size_by_sequence(sequence);
        let mut res = Vec::new();

        'bk: for len in sequence.into_iter() {
            // if any of the dq does not match the `len`, break the loop
            for dq in self.ref_data().into_iter() {
                match dq.front() {
                    Some(a) if a.len() == *len => { /* pass */ }
                    _ => break 'bk,
                }
            }

            let mut chunk_buf = Vec::new();
            for dq in self.mut_data().into_iter() {
                match dq.pop_front() {
                    Some(a) => chunk_buf.push(a),
                    None => break 'bk,
                }
            }

            let chunk = Chunk::new(chunk_buf);
            res.push(chunk);
        }

        res
    }

    // ================================================================================================
    // Functions with different name
    // ================================================================================================

    fn pop_back(&mut self) -> Self::OptDeques {
        self.deque_pop_back()
    }

    fn pop_front(&mut self) -> Self::OptDeques {
        self.deque_pop_front()
    }

    fn push_back<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        self.deque_push_back(value)
    }

    fn push_front<E: Eclectic>(&mut self, value: E) -> FxResult<()> {
        self.deque_push_front(value)
    }
}
