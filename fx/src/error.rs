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

    #[error(transparent)]
    StdIO(std::io::Error),

    #[error(transparent)]
    Sqlx(sqlx::Error),

    #[error(transparent)]
    Arrow(arrow2::error::Error),

    #[error("{0}")]
    ArrowAvro(arrow2::io::avro::avro_schema::error::Error),
}
