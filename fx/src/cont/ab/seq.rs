//! file: seq.rs
//! author: Jacob Xie
//! date: 2023/02/12 16:40:28 Sunday
//! brief: Seq

use std::any::Any;
use std::sync::Arc;

use arrow2::array::{Array, MutableArray};
use arrow2::datatypes::DataType;

// ================================================================================================
// Sqq
// ================================================================================================

pub type ArcArr = Arc<dyn Array>;
pub type ArcMutArr = Arc<dyn MutableArray>;

pub trait FxSeq {
    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> Option<&mut dyn Any>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn data_type(&self) -> &DataType;

    fn get_nulls(&self) -> Option<Vec<bool>>;

    fn is_null(&self, idx: usize) -> Option<bool>;
}

impl FxSeq for ArcArr {
    fn as_any(&self) -> &dyn Any {
        (&**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_any_mut())
    }

    fn len(&self) -> usize {
        (&**self).len()
    }

    fn is_empty(&self) -> bool {
        (&**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (&**self).data_type()
    }

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity()
            .as_ref()
            .map(|bm| bm.iter().map(|i| i).collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }
}

impl FxSeq for ArcMutArr {
    fn as_any(&self) -> &dyn Any {
        (&**self).as_any()
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Arc::get_mut(self).map(|a| a.as_mut_any())
    }

    fn len(&self) -> usize {
        (&**self).len()
    }

    fn is_empty(&self) -> bool {
        (&**self).is_empty()
    }

    fn data_type(&self) -> &DataType {
        (&**self).data_type()
    }

    fn get_nulls(&self) -> Option<Vec<bool>> {
        self.validity()
            .as_ref()
            .map(|bm| bm.iter().map(|i| i).collect())
    }

    fn is_null(&self, idx: usize) -> Option<bool> {
        self.get_nulls().and_then(|e| e.get(idx).copied())
    }
}
