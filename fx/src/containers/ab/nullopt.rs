//! file: nullopt.rs
//! author: Jacob Xie
//! date: 2023/01/30 20:57:20 Monday
//! brief: NullableOptions

use std::collections::HashSet;

use arrow2::datatypes::{DataType, Field, Schema};

use crate::{FxError, FxResult};

#[derive(Clone)]
pub enum NullableOptions {
    None, // True
    True,
    False,
    IndexedTrue(HashSet<usize>),
    VecTrue(Vec<bool>),
}

impl NullableOptions {
    pub fn indexed_true<I>(d: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        Self::IndexedTrue(HashSet::from_iter(d.into_iter()))
    }

    pub fn vec_true<I>(d: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        Self::VecTrue(Vec::from_iter(d))
    }

    pub fn gen_schema<IN, INT, IT>(&self, fields_name: IN, data_types: IT) -> FxResult<Schema>
    where
        IN: IntoIterator<Item = INT>,
        INT: AsRef<str>,
        IT: IntoIterator<Item = DataType>,
    {
        let fn_iter = fields_name.into_iter();
        let dt_iter = data_types.into_iter();
        let (fn_size, dt_size) = (fn_iter.size_hint().0, dt_iter.size_hint().0);
        if fn_size != dt_size {
            return Err(FxError::LengthMismatch(fn_size, dt_size));
        }

        let z = fn_iter.zip(dt_iter);

        let fld = match self {
            NullableOptions::IndexedTrue(hs) => z
                .enumerate()
                .map(|(idx, (n, t))| Field::new(n.as_ref(), t, hs.contains(&idx)))
                .collect::<Vec<_>>(),
            NullableOptions::VecTrue(v) => z
                .enumerate()
                .map(|(idx, (n, t))| Field::new(n.as_ref(), t, v.get(idx).cloned().unwrap_or(true)))
                .collect(),
            opts => z
                .map(|(n, t)| Field::new(n.as_ref(), t, !matches!(opts.clone(), Self::False)))
                .collect(),
        };

        Ok(Schema::from(fld))
    }
}
