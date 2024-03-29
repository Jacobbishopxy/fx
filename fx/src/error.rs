//! Error

use thiserror::Error;

pub type FxResult<T> = Result<T, FxError>;

#[derive(Error, Debug)]
pub enum FxError {
    #[error("must be one of the following: mssql/mysql/postgres")]
    DatabaseTypeNotMatch,

    #[error("connection has already been established")]
    DatabaseConnectionE,

    #[error("connection has not yet established")]
    DatabaseConnectionN,

    #[error("{0}")]
    InvalidArgument(String),

    #[error("{0}")]
    InvalidCasting(String),

    #[error("{0}")]
    InvalidType(String),

    #[error("invalid type")]
    InvalidTypeN,

    #[error("{0}")]
    InvalidOperation(String),

    #[error("invalid downcast")]
    InvalidDowncast,

    #[error("failed to convert")]
    FailedToConvert,

    #[error("datatype mismatch")]
    DatatypeMismatch,

    #[error("schema mismatch")]
    SchemaMismatch,

    #[error("length does not match: ${0} & ${1}")]
    LengthMismatch(usize, usize),

    #[error("empty content")]
    EmptyContent,

    #[error("out of the bounds")]
    OutBounds,

    #[error("key not found")]
    NoKey,

    #[error(transparent)]
    StdIO(std::io::Error),

    #[error(transparent)]
    Sqlx(sqlx::Error),

    #[error(transparent)]
    Arrow(arrow2::error::Error),

    #[error("{0}")]
    ArrowAvro(arrow2::io::avro::avro_schema::error::Error),
}

impl From<std::io::Error> for FxError {
    fn from(e: std::io::Error) -> Self {
        FxError::StdIO(e)
    }
}

impl From<sqlx::Error> for FxError {
    fn from(e: sqlx::Error) -> Self {
        FxError::Sqlx(e)
    }
}

impl From<arrow2::error::Error> for FxError {
    fn from(e: arrow2::error::Error) -> Self {
        FxError::Arrow(e)
    }
}

impl From<arrow2::io::avro::avro_schema::error::Error> for FxError {
    fn from(e: arrow2::io::avro::avro_schema::error::Error) -> Self {
        FxError::ArrowAvro(e)
    }
}
