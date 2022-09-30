//! Connector

use std::future::Future;
use std::str::FromStr;

use futures::future::BoxFuture;
use futures::TryStreamExt;
use sqlx::mssql::{MssqlPool, MssqlPoolOptions, MssqlRow};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Database, FromRow, Mssql, MySql, Pool, Postgres, Row};

use crate::*;

// ================================================================================================
// DB
// ================================================================================================

pub enum DB {
    MsSql(Connector<MssqlPool>),
    MySql(Connector<MySqlPool>),
    Postgres(Connector<PgPool>),
}

impl FromStr for DB {
    type Err = FxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mssql" => Ok(DB::MsSql(Connector::<MssqlPool>::new(s))),
            "mysql" => Ok(DB::MySql(Connector::<MySqlPool>::new(s))),
            "postgres" => Ok(DB::Postgres(Connector::<PgPool>::new(s))),
            _ => Err(FxError::DatabaseTypeNotMatch),
        }
    }
}

pub type PipeFn<I, O> = fn(I) -> FxResult<O>;

// ================================================================================================
// Connector
// ================================================================================================

/// Connector
///
/// Database connector which supports Mssql/Mysql/Postgres.
pub struct Connector<T: SqlMeta> {
    conn_str: String,
    pool_options: Option<T>,
}

impl<T: SqlMeta> Connector<T> {
    pub fn new<S: Into<String>>(conn_str: S) -> Self {
        Self {
            conn_str: conn_str.into(),
            pool_options: None,
        }
    }

    pub async fn connect(&mut self) -> FxResult<()> {
        match self.pool_options.as_ref() {
            None => {
                let p = T::new(&self.conn_str).await?;
                self.pool_options = Some(p);
                Ok(())
            }
            Some(_) => Err(FxError::DatabaseConnectionE),
        }
    }

    pub async fn disconnect(&mut self) -> FxResult<()> {
        match self.pool_options.take() {
            None => Err(FxError::DatabaseConnectionN),
            Some(p) => p.close().await,
        }
    }

    pub async fn query<'a, D>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<<T::DB as Database>::Row, D>,
    ) -> FxResult<Vec<D>>
    where
        D: Send + Unpin + 'a,
    {
        match self.pool_options.as_ref() {
            Some(p) => p.query(sql, pipe).await,
            None => Err(FxError::DatabaseConnectionN),
        }
    }

    pub async fn query_one<'a, D>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<<T::DB as Database>::Row, D>,
    ) -> FxResult<D>
    where
        D: Send + Unpin + 'a,
    {
        match self.pool_options.as_ref() {
            Some(p) => p.query_one(sql, pipe).await,
            None => Err(FxError::DatabaseConnectionN),
        }
    }

    pub async fn query_as<'a, D>(&'a self, sql: &'a str) -> FxResult<Vec<D>>
    where
        D: Send + Unpin + for<'r> FromRow<'r, <T::DB as Database>::Row>,
    {
        match self.pool_options.as_ref() {
            Some(p) => p.query_as(sql).await,
            None => Err(FxError::DatabaseConnectionN),
        }
    }

    pub async fn query_one_as<'a, D>(&'a self, sql: &'a str) -> FxResult<D>
    where
        D: Send + Unpin + for<'r> FromRow<'r, <T::DB as Database>::Row>,
    {
        match self.pool_options.as_ref() {
            Some(p) => p.query_one_as(sql).await,
            None => Err(FxError::DatabaseConnectionN),
        }
    }
}

// ================================================================================================
// SqlMeta
// ================================================================================================

pub trait SqlMeta: Sized {
    // async function's return for self constructor
    type FutSelf<'a>: Future<Output = FxResult<Self>>
    where
        Self: 'a;

    // async function's return without return's type
    type FutNil<'a>: Future<Output = FxResult<()>>
    where
        Self: 'a;

    // trait from `sqlx`, only accepts `Mssql`/`MySql`/`Postgres`
    type DB: Database;

    // constructor
    fn new(conn_str: &str) -> Self::FutSelf<'_>;

    // close connection
    fn close(&self) -> Self::FutNil<'_>;

    // check if connection is closed
    fn is_closed(&self) -> bool;

    // query with a pipe function handling with `Database::Row`
    fn query<'a, D>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<<Self::DB as Database>::Row, D>,
    ) -> BoxFuture<'a, FxResult<Vec<D>>>
    where
        D: Send + Unpin + 'a;

    // query (limit one)
    fn query_one<'a, D>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<<Self::DB as Database>::Row, D>,
    ) -> BoxFuture<'a, FxResult<D>>
    where
        D: Send + Unpin + 'a;

    // query with an explicit type announcement, who implemented `FrowRow`
    fn query_as<'a, D>(&'a self, sql: &'a str) -> BoxFuture<'a, FxResult<Vec<D>>>
    where
        D: Send + Unpin + for<'r> FromRow<'r, <Self::DB as Database>::Row>;

    // query (limit one)
    fn query_one_as<'a, D>(&'a self, sql: &'a str) -> BoxFuture<'a, FxResult<D>>
    where
        D: Send + Unpin + for<'r> FromRow<'r, <Self::DB as Database>::Row>;

    // query with generic param `D` as schema, and return `Datagrid`
    fn query_datagrid<'a, D, const S: usize>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, FxResult<Datagrid>>
    where
        D: Send + Unpin + FxDatagridTypedRowBuild<S>;

    // execute SQL statement without output
    fn execute<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, FxResult<<Self::DB as Database>::QueryResult>>;
}

macro_rules! impl_sql_meta {
    ($db:ident, $db_pool_options:ident, $db_pool:ident) => {
        impl SqlMeta for Pool<$db> {
            type FutSelf<'a> = impl Future<Output = FxResult<Self>>;
            type FutNil<'a> = impl Future<Output = FxResult<()>>;
            type DB = $db;

            fn new(conn_str: &str) -> Self::FutSelf<'_> {
                async move {
                    let po = $db_pool_options::new().connect(conn_str).await?;
                    Ok(po)
                }
            }

            fn close(&self) -> Self::FutNil<'_> {
                async move {
                    $db_pool::close(self).await;
                    Ok(())
                }
            }

            fn is_closed(&self) -> bool {
                $db_pool::is_closed(self)
            }

            fn query<'a, T: Send + Unpin + 'a>(
                &'a self,
                sql: &'a str,
                pipe: PipeFn<<Self::DB as Database>::Row, T>,
            ) -> BoxFuture<'a, FxResult<Vec<T>>> {
                let q = async move {
                    Ok(sqlx::query(sql)
                        .try_map(|r| Ok(pipe(r)))
                        .fetch_all(self)
                        .await?)
                    .and_then(|r| r.into_iter().collect::<FxResult<Vec<T>>>())
                };
                Box::pin(q)
            }

            fn query_one<'a, T: Send + Unpin + 'a>(
                &'a self,
                sql: &'a str,
                pipe: PipeFn<<Self::DB as Database>::Row, T>,
            ) -> BoxFuture<'a, FxResult<T>> {
                let q = async move {
                    Ok(sqlx::query(sql)
                        .try_map(|r| Ok(pipe(r)))
                        .fetch_one(self)
                        .await?)
                    .and_then(|r| r)
                };
                Box::pin(q)
            }

            fn query_as<'a, T: Send + Unpin + for<'r> FromRow<'r, <Self::DB as Database>::Row>>(
                &'a self,
                sql: &'a str,
            ) -> BoxFuture<'a, FxResult<Vec<T>>> {
                let q = async move { Ok(sqlx::query_as::<_, T>(sql).fetch_all(self).await?) };
                Box::pin(q)
            }

            fn query_one_as<
                'a,
                T: Send + Unpin + for<'r> FromRow<'r, <Self::DB as Database>::Row>,
            >(
                &'a self,
                sql: &'a str,
            ) -> BoxFuture<'a, FxResult<T>> {
                let q = async move { Ok(sqlx::query_as::<_, T>(sql).fetch_one(self).await?) };
                Box::pin(q)
            }

            fn query_datagrid<'a, D, const S: usize>(
                &'a self,
                sql: &'a str,
            ) -> BoxFuture<'a, FxResult<Datagrid>>
            where
                D: Send + Unpin + FxDatagridTypedRowBuild<S>,
            {
                let q = async move {
                    let schema = D::schema()?;

                    let mut build = DatagridRowWiseBuilder::new(schema.clone());

                    let mut rows = sqlx::query(sql).fetch(self);

                    while let Some(row) = rows.try_next().await? {
                        let fx_row = schema.row_convert(row)?;

                        build.stack_uncheck(fx_row);
                    }

                    build.build_by_type::<D>()
                };

                Box::pin(q)
            }

            fn execute<'a>(
                &'a self,
                sql: &'a str,
            ) -> BoxFuture<'a, FxResult<<Self::DB as Database>::QueryResult>> {
                let q = async move { Ok(sqlx::query(sql).execute(self).await?) };
                Box::pin(q)
            }
        }
    };
}

impl_sql_meta!(Mssql, MssqlPoolOptions, MssqlPool);
impl_sql_meta!(MySql, MySqlPoolOptions, MySqlPool);
impl_sql_meta!(Postgres, PgPoolOptions, PgPool);

// ================================================================================================
// FxRow conversions ()
// ================================================================================================

pub trait SqlxRowConversion<const S: usize, T> {
    fn row_convert(&self, row: T) -> FxResult<FxRow<S>>;
}

impl<const S: usize> SqlxRowConversion<S, MssqlRow> for FxSchema<S> {
    fn row_convert(&self, row: MssqlRow) -> FxResult<FxRow<S>> {
        let v = self
            .types()
            .iter()
            .enumerate()
            .map(|(idx, t)| match t {
                FxValueType::U8 => Ok(FxValue::U8(row.try_get(idx)?)),
                FxValueType::I8 => Ok(FxValue::I8(row.try_get(idx)?)),
                FxValueType::I16 => Ok(FxValue::I16(row.try_get(idx)?)),
                FxValueType::I32 => Ok(FxValue::I32(row.try_get(idx)?)),
                FxValueType::I64 => Ok(FxValue::I64(row.try_get(idx)?)),
                FxValueType::F32 => Ok(FxValue::F32(row.try_get(idx)?)),
                FxValueType::F64 => Ok(FxValue::F64(row.try_get(idx)?)),
                FxValueType::Bool => Ok(FxValue::Bool(row.try_get(idx)?)),
                FxValueType::String => Ok(FxValue::String(row.try_get(idx)?)),
                _ => Err(FxError::InvalidType(t.to_string())),
            })
            .collect::<FxResult<Vec<_>>>()?;

        FxRow::<S>::try_from(v)
    }
}

impl<const S: usize> SqlxRowConversion<S, MySqlRow> for FxSchema<S> {
    fn row_convert(&self, row: MySqlRow) -> FxResult<FxRow<S>> {
        let v = self
            .types()
            .iter()
            .enumerate()
            .map(|(idx, t)| match t {
                FxValueType::U8 => Ok(FxValue::U8(row.try_get(idx)?)),
                FxValueType::U16 => Ok(FxValue::U16(row.try_get(idx)?)),
                FxValueType::U32 => Ok(FxValue::U32(row.try_get(idx)?)),
                FxValueType::U64 => Ok(FxValue::U64(row.try_get(idx)?)),
                FxValueType::I8 => Ok(FxValue::I8(row.try_get(idx)?)),
                FxValueType::I16 => Ok(FxValue::I16(row.try_get(idx)?)),
                FxValueType::I32 => Ok(FxValue::I32(row.try_get(idx)?)),
                FxValueType::I64 => Ok(FxValue::I64(row.try_get(idx)?)),
                FxValueType::F32 => Ok(FxValue::F32(row.try_get(idx)?)),
                FxValueType::F64 => Ok(FxValue::F64(row.try_get(idx)?)),
                FxValueType::Bool => Ok(FxValue::Bool(row.try_get(idx)?)),
                FxValueType::String => Ok(FxValue::String(row.try_get(idx)?)),
                _ => Err(FxError::InvalidType(t.to_string())),
            })
            .collect::<FxResult<Vec<_>>>()?;

        FxRow::<S>::try_from(v)
    }
}

impl<const S: usize> SqlxRowConversion<S, PgRow> for FxSchema<S> {
    fn row_convert(&self, row: PgRow) -> FxResult<FxRow<S>> {
        let v = self
            .types()
            .iter()
            .enumerate()
            .map(|(idx, t)| match t {
                FxValueType::I8 => Ok(FxValue::I8(row.try_get(idx)?)),
                FxValueType::I16 => Ok(FxValue::I16(row.try_get(idx)?)),
                FxValueType::I32 => Ok(FxValue::I32(row.try_get(idx)?)),
                FxValueType::I64 => Ok(FxValue::I64(row.try_get(idx)?)),
                FxValueType::F32 => Ok(FxValue::F32(row.try_get(idx)?)),
                FxValueType::F64 => Ok(FxValue::F64(row.try_get(idx)?)),
                FxValueType::Bool => Ok(FxValue::Bool(row.try_get(idx)?)),
                FxValueType::String => Ok(FxValue::String(row.try_get(idx)?)),
                _ => Err(FxError::InvalidType(t.to_string())),
            })
            .collect::<FxResult<Vec<_>>>()?;

        FxRow::<S>::try_from(v)
    }
}

#[cfg(test)]
mod test_connector {
    use super::*;

    const URL: &str = "postgres://root:secret@localhost:5432/dev";

    #[tokio::test]
    async fn query_success() {
        #[allow(dead_code)]
        #[derive(Debug)]
        struct User {
            email: String,
            nickname: String,
            hash: String,
            role: String,
        }

        impl User {
            fn new(email: String, nickname: String, hash: String, role: String) -> Self {
                User {
                    email,
                    nickname,
                    hash,
                    role,
                }
            }

            fn from_pg_row(row: PgRow) -> FxResult<Self> {
                let email: String = row.try_get(0)?;
                let nickname: String = row.try_get(1)?;
                let hash: String = row.try_get(2)?;
                let role: String = row.try_get(3)?;

                Ok(Self::new(email, nickname, hash, role))
            }
        }

        let mut ct = Connector::<PgPool>::new(URL);

        ct.connect().await.expect("Connection success");

        let sql = "SELECT * FROM users";

        let res = ct.query(sql, User::from_pg_row).await;

        println!("{:?}", res);

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn query_as_success() {
        #[allow(dead_code)]
        #[derive(sqlx::FromRow, Debug)]
        struct Users {
            email: String,
            nickname: String,
            hash: String,
            role: String,
        }

        let mut ct = Connector::<PgPool>::new(URL);
        ct.connect().await.expect("Connection success");

        let sql = "SELECT * FROM users";

        let res = ct.query_as::<Users>(sql).await;

        println!("{:?}", res);

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn query_datagrid() {
        use futures::TryStreamExt;

        let pg_pool = PgPoolOptions::new().connect(URL).await.unwrap();

        let mut v1 = vec![];
        let mut v2 = vec![];
        let mut v3 = vec![];
        let mut v4 = vec![];

        let mut rows = sqlx::query("SELECT * FROM users").fetch(&pg_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let email: String = row.get(0);
            let nickname: String = row.get(1);
            let hash: String = row.get(2);
            let role: String = row.get(3);

            v1.push(email);
            v2.push(nickname);
            v3.push(hash);
            v4.push(role);
        }

        let dg = Datagrid::try_from(vec![
            FxArray::from(v1),
            FxArray::from(v2),
            FxArray::from(v3),
            FxArray::from(v4),
        ])
        .unwrap();

        println!("{:?}", dg);
    }

    #[tokio::test]
    async fn query_typed_datagrid() {
        use fx_macros::FX;

        let pg_pool = PgPoolOptions::new().connect(URL).await.unwrap();

        #[allow(dead_code)]
        #[derive(FX)]
        struct Users {
            email: String,
            nickname: String,
            hash: String,
            role: String,
        }

        let schema = Users::schema().unwrap();

        let mut build = DatagridRowWiseBuilder::new(schema.clone());

        let mut rows = sqlx::query("SELECT * FROM users").fetch(&pg_pool);

        while let Some(row) = rows.try_next().await.unwrap() {
            let fx_row = schema.row_convert(row).unwrap();

            build.stack_uncheck(fx_row);
        }

        let d = build.build_by_type::<Users>();

        println!("{:?}", d);
    }
}
