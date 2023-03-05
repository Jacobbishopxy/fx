//! file: deque.rs
//! author: Jacob Xie
//! date: 2023/03/04 09:15:59 Saturday
//! brief:

use std::ops::{Deref, Range};
use std::{collections::VecDeque, ops::RangeBounds};

use arrow2::{array::Array, datatypes::DataType};

use super::ArcArr;
use crate::error::{FxError, FxResult};

// ================================================================================================
// DequeArr
// ================================================================================================

// Deque<dyn Array>
pub type DequeArr = Deque<ArcArr>;

// Type alias for iter & iter_mut
pub type DequeIter<'a, A> = std::collections::vec_deque::Iter<'a, A>;
pub type DequeIterMut<'a, A> = std::collections::vec_deque::IterMut<'a, A>;

// ================================================================================================
// Deque
// ================================================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deque<A: AsRef<dyn Array>> {
    datatype: Option<DataType>,
    deque: VecDeque<A>,
}

impl<A: AsRef<dyn Array>> Deque<A> {
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

    /// Provides a mutable reference of A to the element at the given index.
    /// Returns `None` if index out of bounds
    pub fn get_mut(&mut self, index: usize) -> Option<&mut A> {
        self.deque.get_mut(index)
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

    /// Shortens the deque
    pub fn truncate(&mut self, len: usize) {
        self.deque.truncate(len);
    }

    /// Creates an iterator that covers the specified range in the deque
    pub fn range<R>(&self, range: R) -> DequeIter<A>
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

    /// Returns the iter of this [`Deque<A>`].
    pub fn iter(&self) -> DequeIter<A> {
        self.deque.iter()
    }

    /// Returns the iter of this [`Deque<A>`].
    pub fn iter_mut(&mut self) -> DequeIterMut<A> {
        self.deque.iter_mut()
    }
}

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
    iter: DequeIter<'a, A>,
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
// Test
// ================================================================================================

#[cfg(test)]
mod deque_test {
    use crate::row_builder::FromSlice;

    use super::*;

    #[test]
    fn into_arrays_success() {
        let aa = ArcArr::from_slice(&[1, 2, 3]);

        let mut deque = Deque::new(vec![aa]);

        let aa2 = ArcArr::from_slice(&[4, 5]);

        let res = deque.push_back(aa2);
        assert!(res.is_ok());

        let aa3 = ArcArr::from_slice(&[9, 10]);
        let res = deque.push_front(aa3);
        assert!(res.is_ok());

        println!("{:?}", deque.as_slices());

        println!("{:?}", deque.into_arrays());
    }
}
