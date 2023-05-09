//! file: deque.rs
//! author: Jacob Xie
//! date: 2023/03/04 09:15:59 Saturday
//! brief:

use std::ops::{Deref, Range};
use std::{collections::VecDeque, ops::RangeBounds};

use arrow2::{array::Array, datatypes::DataType};

use super::private::{chop_arr, chop_arr_pieces, concat_arr};
use super::{ArcArr, BoxArr};
use crate::error::{FxError, FxResult};

// ================================================================================================
// DequeArr
// ================================================================================================

// Deque<dyn Array>
pub type DequeArcArr = Deque<ArcArr>;
pub type DequeBoxArr = Deque<BoxArr>;

// Type alias for iter & iter_mut
pub type DequeIterRef<'a, A> = std::collections::vec_deque::Iter<'a, A>;
pub type DequeIterMut<'a, A> = std::collections::vec_deque::IterMut<'a, A>;
pub type DequeIterOwned<A> = std::collections::vec_deque::IntoIter<A>;

// ================================================================================================
// Deque
// ================================================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deque<A: AsRef<dyn Array>> {
    datatype: Option<DataType>,
    deque: VecDeque<A>,
}

impl<A: AsRef<dyn Array>> Deque<A> {
    // ============================================================================================
    // private methods
    // ============================================================================================

    // None

    // ============================================================================================
    // public methods
    // ============================================================================================

    /// Creates a new [`Deque`]
    /// # Panic
    /// Iff the arrays do not have the same datatype
    pub fn new(arrays: Vec<A>) -> Self {
        Self::try_new(arrays).unwrap()
    }

    /// Creates a new [`Deque`]
    /// # Error
    /// Iff the arrays do not have the same length
    pub fn try_new(arrays: Vec<A>) -> FxResult<Self> {
        let mut datatype = None;
        match arrays.first() {
            Some(a) => {
                datatype = Some(a.as_ref().data_type().clone());
                if arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .any(|array| array.data_type() != datatype.as_ref().unwrap())
                {
                    Err(FxError::DatatypeMismatch)
                } else {
                    Ok(Self {
                        datatype,
                        deque: VecDeque::from(arrays),
                    })
                }
            }
            None => Ok(Self {
                datatype,
                deque: VecDeque::new(),
            }),
        }
    }

    /// Creates an empty [`Deque<A>`]
    pub fn new_empty() -> Self {
        Self {
            datatype: None,
            deque: VecDeque::new(),
        }
    }

    /// Creates an empty [`Deque<A>`] with a datatype
    pub fn new_empty_with_type(datatype: DataType) -> Self {
        Self {
            datatype: Some(datatype),
            deque: VecDeque::new(),
        }
    }

    pub fn datatype(&self) -> Option<&DataType> {
        self.datatype.as_ref()
    }

    /// Returns the length of this [`Deque<A>`]
    pub fn len(&self) -> usize {
        self.deque.len()
    }

    /// Returns the total arrays length in this [`Deque<A>`].
    pub fn array_len(&self) -> usize {
        self.deque.iter().fold(0, |mut acc, e| {
            acc += e.as_ref().len();
            acc
        })
    }

    /// Returns the len of arrays of this [`Deque<A>`].
    pub fn len_of_arrays(&self) -> Vec<usize> {
        self.deque.iter().map(|a| a.as_ref().len()).collect()
    }

    /// Checks if this [`Deque<A>`] is empty
    pub fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    pub fn has_type(&self) -> bool {
        self.datatype.is_none()
    }

    /// Consumes [`Deque<A>`] into contiguous A
    pub fn into_arrays(self) -> Vec<A> {
        Vec::from(self.deque)
    }

    /// Checks if this [`Deque<A>`] has the same type as input
    pub fn data_type_match(&self, datatype: &DataType) -> bool {
        self.datatype.as_ref().map_or(false, |d| d == datatype)
    }

    /// Returns the arrays of this [`Deque<A>`]
    pub fn as_slices(&self) -> (&[A], &[A]) {
        self.deque.as_slices()
    }

    /// Returns a mutable reference to the make contiguous of this [`Deque<A>`].
    pub fn make_contiguous(&mut self) -> &mut [A] {
        self.deque.make_contiguous()
    }

    /// Returns a reference to the make as slice of this [`Deque<A>`].
    pub fn make_as_slice(&mut self) -> &[A] {
        self.make_contiguous();

        self.as_slices().0
    }

    /// Provides a reference of A to the element at the given index.
    /// Returns `None` if index out of bounds
    pub fn get(&self, index: usize) -> Option<&A> {
        self.deque.get(index)
    }

    pub fn get_ok(&self, index: usize) -> FxResult<&A> {
        self.deque.get(index).ok_or(FxError::OutBounds)
    }

    /// Provides a mutable reference of A to the element at the given index.
    /// Returns `None` if index out of bounds
    pub fn get_mut(&mut self, index: usize) -> Option<&mut A> {
        self.deque.get_mut(index)
    }

    pub fn get_mut_ok(&mut self, index: usize) -> FxResult<&mut A> {
        self.deque.get_mut(index).ok_or(FxError::OutBounds)
    }

    /// Inserts an A at the index
    /// # Errors
    /// This function will return an error if index > self.len() or doesn't hold a type (empty data)
    pub fn insert(&mut self, index: usize, value: A) -> FxResult<()> {
        if index > self.len() || !self.has_type() {
            return Err(FxError::OutBounds);
        }
        if !self.data_type_match(value.as_ref().data_type()) {
            return Err(FxError::DatatypeMismatch);
        }
        self.deque.insert(index, value);

        Ok(())
    }

    /// Returns the back of this [`Deque<A>`]
    pub fn back(&self) -> Option<&A> {
        self.deque.back()
    }

    /// Returns the mutable back of this [`Deque<A>`]
    pub fn back_mut(&mut self) -> Option<&mut A> {
        self.deque.back_mut()
    }

    /// Returns the front of this [`Deque<A>`]
    pub fn front(&self) -> Option<&A> {
        self.deque.front()
    }

    /// Returns the mutable front of this [`Deque<A>`]
    pub fn front_mut(&mut self) -> Option<&mut A> {
        self.deque.front_mut()
    }

    /// Returns the pop back of this [`Deque<A>`]
    pub fn pop_back(&mut self) -> Option<A> {
        self.deque.pop_back()
    }

    /// Returns the pop front of this [`Deque<A>`]
    pub fn pop_front(&mut self) -> Option<A> {
        self.deque.pop_front()
    }

    /// Appends an A to the back of this [`Deque<A>`]
    /// # Errors
    /// This function will return an error if value type mismatch.
    pub fn push_back(&mut self, value: A) -> FxResult<()> {
        if self.is_empty() && !self.has_type() {
            self.datatype = Some(value.as_ref().data_type().clone());
            self.deque.push_back(value);
            return Ok(());
        }
        if self.data_type_match(value.as_ref().data_type()) {
            self.deque.push_back(value);
            Ok(())
        } else {
            Err(FxError::DatatypeMismatch)
        }
    }

    /// Prepends an A to this [`Deque<A>`]
    /// # Errors
    /// This function will return an error if value type mismatch.
    pub fn push_front(&mut self, value: A) -> FxResult<()> {
        if self.is_empty() && !self.has_type() {
            self.datatype = Some(value.as_ref().data_type().clone());
            self.deque.push_front(value);
            return Ok(());
        }
        if self.data_type_match(value.as_ref().data_type()) {
            self.deque.push_front(value);
            Ok(())
        } else {
            Err(FxError::DatatypeMismatch)
        }
    }

    // pub fn pop_many_back(&mut self, num: usize)
    // pub fn pop_many_front(&mut self, num: usize)

    pub fn push_many_back<I>(&mut self, value: I) -> FxResult<()>
    where
        I: IntoIterator<Item = A>,
    {
        for a in value.into_iter() {
            self.push_back(a)?;
        }

        Ok(())
    }

    pub fn push_many_front<I>(&mut self, value: I) -> FxResult<()>
    where
        I: IntoIterator<Item = A>,
    {
        for a in value.into_iter() {
            self.push_front(a)?;
        }

        Ok(())
    }

    pub fn remove(&mut self, index: usize) -> Option<A> {
        self.deque.remove(index)
    }

    /// Shortens the deque
    pub fn truncate(&mut self, len: usize) {
        self.deque.truncate(len);
    }

    /// Creates an iterator that covers the specified range in the deque
    pub fn range<R>(&self, range: R) -> DequeIterRef<A>
    where
        R: RangeBounds<usize>,
    {
        self.deque.range(range)
    }

    /// Creates an iterator that covers the specified mutable range in the deque
    pub fn range_mut<R>(&mut self, range: R) -> DequeIterMut<A>
    where
        R: RangeBounds<usize>,
    {
        self.deque.range_mut(range)
    }

    /// Returns a slice to this [`Deque<A>`]
    /// # Errors
    /// This function will return an error if offset + length > self.len()
    pub fn slice(&self, offset: usize, length: usize) -> FxResult<Vec<&A>> {
        if offset + length > self.len() {
            return Err(FxError::OutBounds);
        }

        let iter = self.range(Range {
            start: offset,
            end: offset + length,
        });

        Ok(iter.collect())
    }

    /// Returns a mutable slice to this [`Deque<A>`]
    /// # Errors
    /// This function will return an error if offset + length > self.len()
    pub fn slice_mut(&mut self, offset: usize, length: usize) -> FxResult<Vec<&mut A>> {
        if offset + length > self.len() {
            return Err(FxError::OutBounds);
        }

        let iter = self.range_mut(Range {
            start: offset,
            end: offset + length,
        });

        Ok(iter.collect())
    }

    /// Returns the reference iter of this [`Deque<A>`].
    pub fn iter(&self) -> DequeIterRef<A> {
        self.deque.iter()
    }

    /// Returns the mutable reference iter of this [`Deque<A>`].
    pub fn iter_mut(&mut self) -> DequeIterMut<A> {
        self.deque.iter_mut()
    }

    /// Returns the ownership iter of this [`Deque<A>`].
    pub fn iter_owned(self) -> DequeIterOwned<A> {
        self.deque.into_iter()
    }
}

// ================================================================================================
// From & Deref
// ================================================================================================

impl<A: AsRef<dyn Array>> From<Deque<A>> for Vec<A> {
    fn from(q: Deque<A>) -> Self {
        q.into_arrays()
    }
}

impl<A: AsRef<dyn Array>> Deref for Deque<A> {
    type Target = VecDeque<A>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.deque
    }
}

impl<A: AsRef<dyn Array>> From<A> for Deque<A> {
    fn from(a: A) -> Self {
        Deque::new(vec![a])
    }
}

// ================================================================================================
// Iterator: Ref
// ================================================================================================

/// impl IntoIterator for ref [`Deque<A>`]
impl<'a, A: AsRef<dyn Array>> IntoIterator for &'a Deque<A> {
    type Item = &'a A;

    type IntoIter = DequeRefIterator<'a, A>;

    fn into_iter(self) -> Self::IntoIter {
        DequeRefIterator { iter: self.iter() }
    }
}

/// Ref iterator
pub struct DequeRefIterator<'a, A: AsRef<dyn Array>> {
    iter: DequeIterRef<'a, A>,
}

/// impl Iterator for [`DequeRefIterator`]
impl<'a, A: AsRef<dyn Array>> Iterator for DequeRefIterator<'a, A> {
    type Item = &'a A;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// ================================================================================================
// Iterator: Mut
// ================================================================================================

/// impl IntoIterator for mut ref [`Deque<A>`]
impl<'a, A: AsRef<dyn Array>> IntoIterator for &'a mut Deque<A> {
    type Item = &'a mut A;

    type IntoIter = DequeMutIterator<'a, A>;

    fn into_iter(self) -> Self::IntoIter {
        DequeMutIterator {
            iter: self.iter_mut(),
        }
    }
}

/// Mut iterator
pub struct DequeMutIterator<'a, A: AsRef<dyn Array>> {
    iter: DequeIterMut<'a, A>,
}

/// impl Iterator for [`DequeMutIterator`]
impl<'a, A: AsRef<dyn Array>> Iterator for DequeMutIterator<'a, A> {
    type Item = &'a mut A;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// ================================================================================================
// Iterator: Owned
// ================================================================================================

/// impl IntoIterator for mut ref [`Deque<A>`]
impl<A: AsRef<dyn Array>> IntoIterator for Deque<A> {
    type Item = A;

    type IntoIter = DequeOwnedIterator<A>;

    fn into_iter(self) -> Self::IntoIter {
        DequeOwnedIterator {
            iter: self.iter_owned(),
        }
    }
}

/// Mut iterator
pub struct DequeOwnedIterator<A: AsRef<dyn Array>> {
    iter: DequeIterOwned<A>,
}

/// impl Iterator for [`DequeOwnedIterator`]
impl<A: AsRef<dyn Array>> Iterator for DequeOwnedIterator<A> {
    type Item = A;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// ================================================================================================
// Make same size
// ================================================================================================

impl<A> Deque<A>
where
    A: AsRef<dyn Array> + From<BoxArr>,
{
    /// Make every Array into the same size, and the residual at the end
    pub fn size_arrays_equally(&mut self, len: usize) -> SameSizedResult {
        let total_length = self.array_len();
        // take deque and convert it into `Vec` type
        let d = Vec::from(std::mem::take(&mut self.deque));

        // concat all
        if len >= total_length {
            let d = concat_arr(&d).unwrap();
            self.deque.push_back(d);

            return SameSizedResult {
                each_array_size: total_length,
                residual_array_size: 0,
                total_array_num: 1,
            };
        }

        // collect `A` whose size is less then `len`
        let mut buffer = Vec::<A>::new();
        // the total length of `A`s in the buffer
        let mut cur_buffer_total_len = 0;
        // result
        let mut res = Vec::<A>::new();

        // iterate through `d`
        for arr in d.into_iter() {
            let arr_len = arr.as_ref().len();
            cur_buffer_total_len += arr_len;

            // collecting `A`s until the `buffer`'s total len is greater than `len`
            if cur_buffer_total_len < len {
                buffer.push(arr);
                continue;
            } else {
                // the chopped length of the right `A`
                let r_len = cur_buffer_total_len - len;
                let (l, r) = chop_arr(arr, arr_len - r_len).unwrap();
                // till now the `buffer` meets the required length, and concatenate them into one `A`
                buffer.push(l);
                let concat = concat_arr(&buffer).unwrap();
                res.push(concat);
                // clear buffer and reset buffer_total_len's count
                buffer.clear();
                cur_buffer_total_len = 0;

                // chop the right `A` into pieces
                let mut slices = chop_arr_pieces(r, len);

                // the last part of slices is the residual
                if let Some(a) = slices.pop() {
                    // handle the well sliced part first
                    res.extend(slices);
                    // if the residual is less then `len`, then cache it into the `buffer`;
                    // otherwise, push it into the `res`
                    let a_len = a.as_ref().len();
                    if a_len == 0 {
                        continue;
                    } else if a_len < len {
                        buffer.push(a);
                        cur_buffer_total_len += a_len;
                    } else {
                        res.push(a);
                    }
                }
            }
        }

        // handle the residual in the `buffer`
        if !buffer.is_empty() {
            res.push(concat_arr(&buffer).unwrap());
        }

        self.deque = VecDeque::from(res);

        SameSizedResult {
            each_array_size: len,
            residual_array_size: total_length % len,
            total_array_num: self.len(),
        }
    }

    /// Make Array follows the sizes of the input `sequence`,
    pub fn size_arrays_by_sequence<'a, I>(&mut self, sequence: &'a I) -> SequenceSizedResult
    where
        &'a I: IntoIterator<Item = &'a usize>,
    {
        let mut sequence = sequence.into_iter();
        // if the `sequence` is empty, then early return
        let mut cur_sequence_num = if let Some(csn) = sequence.next() {
            csn
        } else {
            return SequenceSizedResult {
                array_sizes: self.len_of_arrays(),
                total_array_num: self.len(),
            };
        };

        // same definition as the `size_arrays_equally` method
        let mut buffer = Vec::<A>::new();
        let mut cur_buffer_total_len = 0;
        let mut res = Vec::<A>::new();

        // `pop_front` the `deque` until it is empty or early exits if the `sequence` is empty
        while !self.deque.is_empty() {
            // handle with the first `A`
            let arr = self.pop_front().unwrap();
            let arr_len = arr.as_ref().len();
            cur_buffer_total_len += arr_len;

            // collecting `A`s until the `buffer`'s total len is greater than `cur_sequence_num`
            if cur_buffer_total_len < *cur_sequence_num {
                buffer.push(arr);
                continue;
            } else {
                // same operation as the `size_arrays_equally` method
                let r_len = cur_buffer_total_len - cur_sequence_num;
                let (l, r) = chop_arr(arr, arr_len - r_len).unwrap();
                buffer.push(l);
                let concat = concat_arr(&buffer).unwrap();
                res.push(concat);
                buffer.clear();
                cur_buffer_total_len = 0;

                // handle the rest part of the chopped arr in the next loop
                if !r.as_ref().is_empty() {
                    self.deque.push_front(r);
                }
                // if sequence ends up earlier than `deque`'s operation, break the loop
                match sequence.next() {
                    Some(l) => cur_sequence_num = l,
                    None => break,
                }
            }
        }

        // if sequence ends up earlier, take the rest of the `deque` and concat them with the `buffer`
        if !self.deque.is_empty() {
            let rest = Vec::from(std::mem::take(&mut self.deque));
            buffer.extend(rest);
        }

        if !buffer.is_empty() {
            let concat = concat_arr(&buffer).unwrap();
            res.push(concat);
        }

        self.deque = VecDeque::from(res);

        SequenceSizedResult {
            array_sizes: self.len_of_arrays(),
            total_array_num: self.len(),
        }
    }
}

// ================================================================================================
// Misc
// ================================================================================================

#[derive(Debug)]
pub struct SameSizedResult {
    pub each_array_size: usize,
    pub residual_array_size: usize,
    pub total_array_num: usize,
}

#[derive(Debug)]
pub struct SequenceSizedResult {
    pub array_sizes: Vec<usize>,
    pub total_array_num: usize,
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod deque_test {

    use super::*;

    use crate::ab::FromSlice;
    use crate::{arc_arr, box_arr};

    #[test]
    fn into_arrays_success() {
        let aa = ArcArr::from_slice([1, 2, 3]);

        let mut deque = Deque::new(vec![aa]);

        let aa2 = ArcArr::from_slice([4, 5]);

        let res = deque.push_back(aa2);
        assert!(res.is_ok());

        let aa3 = ArcArr::from_slice([9, 10]);
        let res = deque.push_front(aa3);
        assert!(res.is_ok());

        println!("{:?}", deque.as_slices());

        println!("{:?}", deque.into_arrays());
    }

    #[test]
    fn size_arrays_equally_success1() {
        let mut dq = DequeArcArr::new(vec![
            arc_arr!([1, 2, 3]),
            arc_arr!([1, 2, 3, 4, 5, 6]),
            arc_arr!([1, 2, 3, 4]),
            arc_arr!([1, 2]),
        ]);

        let res = dq.size_arrays_equally(3);
        println!("{:?}", res);
        println!("{:?}", dq);

        let res = dq.size_arrays_equally(4);
        println!("{:?}", res);
        println!("{:?}", dq);

        let res = dq.size_arrays_equally(100);
        println!("{:?}", res);
        println!("{:?}", dq);
    }

    #[test]
    fn size_arrays_equally_success2() {
        let mut dq = DequeBoxArr::new(vec![
            box_arr!([1, 2, 3]),
            box_arr!([1, 2, 3, 4, 5, 6]),
            box_arr!([1, 2, 3, 4]),
            box_arr!([1, 2]),
        ]);

        let res = dq.size_arrays_equally(3);
        println!("{:?}", res);
        println!("{:?}", dq);

        let res = dq.size_arrays_equally(4);
        println!("{:?}", res);
        println!("{:?}", dq);

        let res = dq.size_arrays_equally(100);
        println!("{:?}", res);
        println!("{:?}", dq);
    }

    #[test]
    fn size_arrays_by_sequence_success1() {
        let mut dq = DequeArcArr::new(vec![
            arc_arr!([1, 2, 3]),
            arc_arr!([1, 2, 3, 4, 5, 6]),
            arc_arr!([1, 2, 3, 4]),
            arc_arr!([1, 2]),
            arc_arr!([1, 2, 3]),
            arc_arr!([1]),
            arc_arr!([1, 2, 3, 4, 5, 6, 7]),
            arc_arr!([1, 2]),
        ]);

        let seq = vec![2, 3, 4, 5];
        let res = dq.size_arrays_by_sequence(&seq);
        println!("{:?}", res);
        println!("{:?}", dq);

        let seq = vec![2, 3, 4, 5, 6, 8];
        let res = dq.size_arrays_by_sequence(&seq);
        println!("{:?}", res);
        println!("{:?}", dq);
    }

    #[test]
    fn size_arrays_by_sequence_success2() {
        let mut dq = DequeBoxArr::new(vec![
            box_arr!([1, 2, 3]),
            box_arr!([1, 2, 3, 4, 5, 6]),
            box_arr!([1, 2, 3, 4]),
            box_arr!([1, 2]),
            box_arr!([1, 2, 3]),
            box_arr!([1]),
            box_arr!([1, 2, 3, 4, 5, 6, 7]),
            box_arr!([1, 2]),
        ]);

        let seq = vec![2, 3, 4, 5];
        let res = dq.size_arrays_by_sequence(&seq);
        println!("{:?}", res);
        println!("{:?}", dq);

        let seq = vec![2, 3, 4, 5, 6, 8];
        let res = dq.size_arrays_by_sequence(&seq);
        println!("{:?}", res);
        println!("{:?}", dq);
    }
}
