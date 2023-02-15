//! file: table.rs
//! author: Jacob Xie
//! date: 2023/02/14 17:25:37 Tuesday
//! brief: Table

use arrow2::datatypes::{Field, Schema};

use crate::{FxError, FxResult};

use super::ab::{private, FxSeq};

// ================================================================================================
// FxTable
// ================================================================================================

/// A scalable FxSeq container
#[derive(Debug, Clone)]
pub struct FxTable<S: FxSeq> {
    schema: Schema,
    data: Vec<S>,
}

impl<S> FxTable<S>
where
    S: FxSeq,
{
    /// private method, use `new` & `new_with_names` for public constructors
    fn _new<I, T>(data: Vec<S>, names: Option<I>) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        // default columns names, based on data's length
        let cols = (0..data.len()).map(|i| format!("Col_{i:?}"));

        let names = match names {
            Some(ns) => {
                let mut ns = ns
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
            None => cols.collect(),
        };

        let fields = data
            .iter()
            .zip(names)
            .map(|(d, n)| Field::new(n, d.data_type().clone(), d.has_null()))
            .collect::<Vec<_>>();

        let schema = Schema::from(fields);

        Self { schema, data }
    }

    pub fn new(data: Vec<S>) -> Self {
        FxTable::_new(data, Option::<&[&str]>::None)
    }

    pub fn new_with_names<I, T>(data: Vec<S>, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        FxTable::_new(data, Some(names))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn data(&self) -> &Vec<S> {
        &self.data
    }
}

// ================================================================================================
// Impl private::InnerSheaf for Vec<FxSeq>
// ================================================================================================

impl<S> private::InnerSheaf for Vec<S>
where
    S: FxSeq,
{
    type Seq = S;

    fn empty() -> Self
    where
        Self: Sized,
    {
        Vec::<S>::new()
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        self.as_slice()
    }

    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        self.as_mut_slice()
    }

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        if !self.data_types_match(&arrays) {
            return Err(FxError::SchemaMismatch);
        }

        *self = arrays;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self
    }

    fn take_chunk(self) -> arrow2::chunk::Chunk<crate::types::ArcArr> {
        todo!()
    }
}

// ================================================================================================
// Impl private::InnerSheaf for FxTable
// ================================================================================================

impl<S> private::InnerSheaf for FxTable<S>
where
    S: FxSeq,
{
    type Seq = S;

    fn empty() -> Self
    where
        Self: Sized,
    {
        Self {
            schema: Schema::default(),
            data: vec![],
        }
    }

    fn ref_sequences(&self) -> &[Self::Seq] {
        &self.data
    }

    fn mut_sequences(&mut self) -> &mut [Self::Seq] {
        &mut self.data
    }

    fn set_sequences(&mut self, arrays: Vec<Self::Seq>) -> FxResult<()> {
        if !self.data_types_match(&arrays) {
            return Err(FxError::SchemaMismatch);
        }

        self.data = arrays;

        Ok(())
    }

    fn take_sequences(self) -> Vec<Self::Seq> {
        self.data
    }

    fn take_chunk(self) -> arrow2::chunk::Chunk<crate::types::ArcArr> {
        todo!()
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_table {
    use super::*;

    use crate::{cont::ab::Sheaf, FxArray, FxVector};

    #[test]
    fn create_new_table_succes() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        let table = FxTable::new(vaa);

        println!("{table:?}",);
    }

    #[test]
    fn create_new_table_name_less_succes() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        let table = FxTable::new_with_names(vaa, ["1"]);

        println!("{table:?}",);
    }

    #[test]
    fn create_new_table_name_more_succes() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        let table = FxTable::new_with_names(vaa, ["1", "2", "3"]);

        println!("{table:?}",);
    }

    #[test]
    fn test_vec_of_arc_arr() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        println!("{:?}", vaa.lens());
        println!("{:?}", vaa.data_types());
    }

    #[test]
    fn test_vec_of_arc_vec() {
        let a = FxVector::from(vec![None, Some(1)]).into_mutable_array();
        let b = FxVector::from(vec![None, Some("y"), None]).into_mutable_array();

        let vam = vec![a, b];

        println!("{:?}", vam.lens());
        println!("{:?}", vam.data_types());
    }

    #[test]
    fn test_table_of_arc_arr() {
        let a = FxArray::from(vec![None, Some("x")]).into_array();
        let b = FxArray::from(vec![None, Some(2), None]).into_array();

        let vaa = vec![a, b];

        let table = FxTable::new(vaa);

        println!("{table:?}");
    }

    #[test]
    fn test_table_of_arc_vec() {
        let a = FxVector::from(vec![None, Some(1)]).into_mutable_array();
        let b = FxVector::from(vec![None, Some("y"), None]).into_mutable_array();

        let vam = vec![a, b];

        let table = FxTable::new_with_names(vam, ["a"]);

        println!("{table:?}");
    }
}
