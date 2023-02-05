//! Connector

use std::future::Future;
use std::str::FromStr;

use futures::future::BoxFuture;
use futures::TryStreamExt;
use sqlx::mssql::{MssqlPool, MssqlPoolOptions, MssqlRow};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Database, FromRow, Mssql, MySql, Postgres, Row};

use crate::*;

pub type PipeFn<I, O> = fn(I) -> FxResult<O>;

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

    pub async fn query_grid<'a, D>(&'a self, sql: &'a str) -> FxResult<FxGrid>
    where
        D: Send + FxContainerRowBuilderGenerator,
        D: From<T::Row>,
    {
        match self.pool_options.as_ref() {
            Some(p) => p.query_grid::<D>(sql).await,
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

    type Row: Row;

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

    // query with generic param `D` as schema, and return `FxGrid`
    fn query_grid<'a, D>(&'a self, sql: &'a str) -> BoxFuture<'a, FxResult<FxGrid>>
    where
        D: From<Self::Row>,
        D: Send + FxContainerRowBuilderGenerator;

    // execute SQL statement without output
    fn execute<'a>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, FxResult<<Self::DB as Database>::QueryResult>>;
}

impl_sql_meta!(Mssql, MssqlRow, MssqlPoolOptions, MssqlPool);
impl_sql_meta!(MySql, MySqlRow, MySqlPoolOptions, MySqlPool);
impl_sql_meta!(Postgres, PgRow, PgPoolOptions, PgPool);

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

        println!("{res:?}");

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

        println!("{res:?}");

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn query_grid() {
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

        let dg = FxGrid::try_from(vec![
            FxArray::from(v1),
            FxArray::from(v2),
            FxArray::from(v3),
            FxArray::from(v4),
        ])
        .unwrap();

        println!("{dg:?}");
    }

    #[tokio::test]
    async fn query_typed_grid_success() {
        #[allow(dead_code)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        impl From<PgRow> for Users {
            fn from(v: PgRow) -> Self {
                Users {
                    id: v.get(0),
                    name: v.get(1),
                    check: v.get(2),
                }
            }
        }

        #[derive(Default)]
        struct UsersBuild {
            id: Vec<i32>,
            name: Vec<String>,
            check: Vec<Option<bool>>,
        }

        impl FxContainerRowBuilder<Users> for UsersBuild {
            fn new() -> Self {
                Self::default()
            }

            fn stack(&mut self, row: Users) {
                self.id.push(row.id);
                self.name.push(row.name);
                self.check.push(row.check);
            }

            fn build(self: Box<Self>) -> FxResult<FxGrid> {
                let mut builder = FxGridColWiseBuilder::<3>::new();

                builder.stack(self.id);
                builder.stack(self.name);
                builder.stack(self.check);

                builder.build()
            }
        }

        impl FxContainerRowBuilderGenerator for Users {
            fn gen_row_builder() -> Box<dyn FxContainerRowBuilder<Self>> {
                Box::new(UsersBuild::new())
            }
        }

        let pg_pool = PgPoolOptions::new().connect(URL).await.unwrap();

        let sql = "SELECT * FROM users";

        let res = pg_pool.query_grid::<Users>(sql).await;

        println!("{res:?}");

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn query_auto_derived_grid_success() {
        use crate::FX;

        #[allow(dead_code)]
        #[derive(FX)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        let pg_pool = PgPoolOptions::new().connect(URL).await.unwrap();

        let sql = "SELECT * FROM users";

        let res = pg_pool.query_grid::<Users>(sql).await;

        println!("{res:?}");

        assert!(res.is_ok());
    }
}
