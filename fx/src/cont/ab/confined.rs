//! file: confined.rs
//! author: Jacob Xie
//! date: 2023/03/04 22:51:04 Saturday
//! brief:

use arrow2::datatypes::DataType;

use super::Purport;

// ================================================================================================
// Confined
//
// Container's width and datatype
// ================================================================================================

pub trait Confined {
    fn width(&self) -> usize;

    fn data_types(&self) -> Vec<&DataType>;

    fn data_types_match<T>(&self, d: &T) -> bool
    where
        T: Confined,
    {
        self.width() == d.width() && self.data_types() == d.data_types()
    }
}

impl<T> Confined for T
where
    T: Purport,
{
    fn width(&self) -> usize {
        self.schema().fields.len()
    }

    fn data_types(&self) -> Vec<&DataType> {
        self.schema().fields.iter().map(|f| f.data_type()).collect()
    }
}
