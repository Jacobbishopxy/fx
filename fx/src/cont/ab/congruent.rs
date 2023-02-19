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
    fn take_longest_to_chunk(self) -> FxResult<Chunk<ArcArr>>
    where
        Self: Sized,
    {
        let len = self.max_len().ok_or(FxError::EmptyContent)?;

        let vec_arc_arr = self
            .take_sequences()
            .into_iter()
            .map(|s| {
                s.to_array().and_then(|mut arr| {
                    let missing = len - arr.len();
                    // fill missing by None
                    if missing > 0 {
                        arr.extend(&ArcArr::new_nulls(arr.data_type().clone(), missing))?;
                    }

                    Ok(arr)
                })
            })
            .collect::<FxResult<Vec<_>>>()?;

        Ok(Chunk::try_new(vec_arc_arr)?)
    }

    fn take_shortest_to_chunk(self) -> FxResult<Chunk<ArcArr>>
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

    fn take_len_to_chunk(self, len: usize) -> FxResult<Chunk<ArcArr>>
    where
        Self: Sized,
    {
        let vec_arc_arr = self
            .take_sequences()
            .into_iter()
            .map(|s| {
                s.to_array().and_then(|mut arr| {
                    let al = arr.len();
                    // case: missing
                    if len > al {
                        arr.extend(&ArcArr::new_nulls(arr.data_type().clone(), len - al))?;
                    }
                    // case: over-length
                    if len < al {
                        arr = Arc::from(arr.slice(0, len));
                    }

                    Ok(arr)
                })
            })
            .collect::<FxResult<Vec<_>>>()?;

        Ok(Chunk::try_new(vec_arc_arr)?)
    }
}
