//! Row

use crate::{FxError, FxResult, FxValue, FxValueType};

// ================================================================================================
// Row
// ================================================================================================

#[derive(Debug)]
pub struct FxRow<const S: usize>([FxValue; S]);

impl<const S: usize> FxRow<S> {
    pub fn values(&self) -> &[FxValue] {
        &self.0
    }

    pub fn types(&self) -> Vec<FxValueType> {
        self.0.as_ref().iter().map(FxValueType::from).collect()
    }
}

impl<const S: usize> TryFrom<Vec<FxValue>> for FxRow<S> {
    type Error = FxError;

    fn try_from(value: Vec<FxValue>) -> Result<Self, Self::Error> {
        let len = value.len();
        let arr: FxResult<[FxValue; S]> = value
            .try_into()
            .map_err(|_| FxError::InvalidArgument(format!("invalid length {len}")));

        Ok(Self(arr?))
    }
}

// ================================================================================================
// Schema
// ================================================================================================

#[derive(Debug)]
pub struct FxSchema<const S: usize>([FxValueType; S]);

impl<const S: usize> FxSchema<S> {
    pub fn types(&self) -> &[FxValueType] {
        &self.0
    }

    pub fn check_schema(&self, row: &FxRow<S>) -> bool {
        row.types().as_slice() == self.types()
    }
}

impl<const S: usize> TryFrom<Vec<FxValueType>> for FxSchema<S> {
    type Error = FxError;

    fn try_from(value: Vec<FxValueType>) -> Result<Self, Self::Error> {
        let len = value.len();
        let arr: FxResult<[FxValueType; S]> = value
            .try_into()
            .map_err(|_| FxError::InvalidArgument(format!("invalid length {len}")));

        Ok(Self(arr?))
    }
}
