//! file: congruent.rs
//! author: Jacob Xie
//! date: 2023/02/18 00:22:20 Saturday
//! brief:

use std::sync::Arc;

use arrow2::chunk::Chunk;

use super::{Eclectic, FxSeq};
use crate::{types::ArcArr, FxError, FxResult};

// ================================================================================================
// InnerCongruent
//
// A genetic purpose of Chunk
// ================================================================================================

pub trait Congruent: Eclectic {
    fn take_longest(self) -> FxResult<Chunk<ArcArr>>
    where
        Self: Sized,
    {
        let _len = self.max_len().ok_or(FxError::EmptyContent)?;

        // fill Nan by None

        todo!()
    }

    fn take_shortest(self) -> FxResult<Chunk<ArcArr>>
    where
        Self: Sized,
    {
        let len = self.min_len().ok_or(FxError::EmptyContent)?;

        let vec_arc_arr = self
            .take_sequences()
            .into_iter()
            .map(|s| {
                s.to_array().map(|arr| {
                    // no panic, Box<dyn Array>
                    Arc::from(arr.slice(0, len))
                })
            })
            .collect::<FxResult<Vec<_>>>()?;

        Ok(Chunk::try_new(vec_arc_arr)?)
    }

    fn take_len(self, _len: usize) -> FxResult<Chunk<ArcArr>>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}
