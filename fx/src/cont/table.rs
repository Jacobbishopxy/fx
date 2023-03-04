//! file: table.rs
//! author: Jacob Xie
//! date: 2023/03/04 01:31:56 Saturday
//! brief:

use arrow2::datatypes::Schema;
use inherent::inherent;

use super::{ArcArr, DequeArr};
use crate::ab::{Purport, StaticPurport};
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

    pub fn data(&self) -> &[DequeArr; W] {
        &self.data
    }
}
