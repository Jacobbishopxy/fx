//! file: purport.rs
//! author: Jacob Xie
//! date: 2023/02/15 23:04:38 Wednesday
//! brief: Purport

// ================================================================================================
// Purport
//
// Schema related functions
// ================================================================================================

use arrow2::datatypes::{Field, Schema};

use super::FxSeq;

#[inline]
fn default_cols(len: usize) -> impl Iterator<Item = String> {
    (0..len).map(|i| format!("Col_{i:?}"))
}

#[inline]
fn filled_cols<I, T>(len: usize, names: I) -> Vec<String>
where
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
{
    // default columns names, based on data's length
    let cols = default_cols(len);

    let mut ns = names
        .into_iter()
        .map(|e| e.as_ref().to_string())
        .collect::<Vec<_>>();
    let (ns_size, cl_size) = (ns.len(), cols.size_hint().0);

    // if names' length is shorter than data's length, then use defual `cols` to fill the empties
    if ns_size < cl_size {
        ns.extend(cols.skip(ns_size).collect::<Vec<_>>())
    }
    // another situation is when names' lenght is greater than data's length, whereas the following
    // `data.iter().zip(names)` would only iterate through the shortest iterator. Hence, there is
    // no need to handle the rest of situations (greater or equal).

    ns
}

#[inline]
fn gen_schema<S>(seq: &[S], names: Vec<String>) -> Schema
where
    S: FxSeq,
{
    let fields = seq
        .iter()
        .zip(names)
        .map(|(d, n)| Field::new(n, d.data_type().clone(), d.has_null()))
        .collect::<Vec<_>>();

    Schema::from(fields)
}

pub trait Purport {
    fn schema(&self) -> &Schema;

    // static methods

    fn gen_schema<S>(seq: &[S]) -> Schema
    where
        S: FxSeq,
    {
        gen_schema(seq, default_cols(seq.len()).collect())
    }

    fn gen_schema_with_names<S, I, T>(seq: &[S], names: I) -> Schema
    where
        S: FxSeq,
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        gen_schema(seq, filled_cols(seq.len(), names))
    }
}
