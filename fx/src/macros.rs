//! file:	macros.rs
//! author:	Jacob Xie
//! date:	2023/01/13 22:42:09 Friday
//! brief:	Macros

// ================================================================================================
// impl from native
// ================================================================================================

macro_rules! arr_impl_from_native {
    ($t:ty) => {
        impl From<Vec<$t>> for $crate::FxArray {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(::arrow2::array::PrimitiveArray::from(v).boxed())
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxArray {
            fn from(vec: Vec<Option<$t>>) -> Self {
                Self(::arrow2::array::PrimitiveArray::from(vec).boxed())
            }
        }

        impl $crate::FromSlice<$t, $crate::FxArray> for $crate::FxArray {
            fn from_slice(slice: &[$t]) -> Self {
                Self(::arrow2::array::PrimitiveArray::from_slice(slice).boxed())
            }
        }
    };
}

macro_rules! vec_impl_from_native {
    ($t:ty) => {
        impl From<Vec<$t>> for $crate::FxVector {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(Box::new(::arrow2::array::MutablePrimitiveArray::from(v)))
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxVector {
            fn from(vec: Vec<Option<$t>>) -> Self {
                Self(Box::new(::arrow2::array::MutablePrimitiveArray::from(vec)))
            }
        }

        impl $crate::FromSlice<$t, $crate::FxVector> for $crate::FxVector {
            fn from_slice(slice: &[$t]) -> Self {
                Self(Box::new(
                    ::arrow2::array::MutablePrimitiveArray::from_slice(slice),
                ))
            }
        }
    };
}

pub(crate) use arr_impl_from_native;
pub(crate) use vec_impl_from_native;

// ================================================================================================
// impl from str
// ================================================================================================

macro_rules! arr_impl_from_str {
    ($t:ty) => {
        impl From<Vec<$t>> for $crate::FxArray {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(::arrow2::array::Utf8Array::<i32>::from(v).boxed())
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxArray {
            fn from(vec: Vec<Option<$t>>) -> Self {
                Self(::arrow2::array::Utf8Array::<i32>::from(vec).boxed())
            }
        }

        impl $crate::FromSlice<$t, $crate::FxArray> for $crate::FxArray {
            fn from_slice(slice: &[$t]) -> Self {
                Self(::arrow2::array::Utf8Array::<i32>::from_slice(slice).boxed())
            }
        }
    };
}

macro_rules! vec_impl_from_str {
    ($t:ty) => {
        impl From<Vec<$t>> for $crate::FxVector {
            fn from(vec: Vec<$t>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(Box::new(::arrow2::array::MutableUtf8Array::<i32>::from(v)))
            }
        }

        impl From<Vec<Option<$t>>> for $crate::FxVector {
            fn from(vec: Vec<Option<$t>>) -> Self {
                Self(Box::new(::arrow2::array::MutableUtf8Array::<i32>::from(
                    vec,
                )))
            }
        }

        impl $crate::FromSlice<$t, $crate::FxVector> for $crate::FxVector {
            fn from_slice(slice: &[$t]) -> Self {
                Self(Box::new(
                    ::arrow2::array::MutableUtf8Array::<i32>::from_iter_values(slice.into_iter()),
                ))
            }
        }
    };
}

pub(crate) use arr_impl_from_str;
pub(crate) use vec_impl_from_str;

// ================================================================================================
// impl from bool
// ================================================================================================

macro_rules! arr_impl_from_bool {
    () => {
        impl From<Vec<bool>> for $crate::FxArray {
            fn from(vec: Vec<bool>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(::arrow2::array::BooleanArray::from(v).boxed())
            }
        }

        impl From<Vec<Option<bool>>> for $crate::FxArray {
            fn from(vec: Vec<Option<bool>>) -> Self {
                Self(::arrow2::array::BooleanArray::from(vec).boxed())
            }
        }

        impl FromSlice<bool, $crate::FxArray> for $crate::FxArray {
            fn from_slice(slice: &[bool]) -> Self {
                Self(::arrow2::array::BooleanArray::from_slice(slice).boxed())
            }
        }
    };
}

macro_rules! vec_impl_from_bool {
    () => {
        impl From<Vec<bool>> for $crate::FxVector {
            fn from(vec: Vec<bool>) -> Self {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                Self(Box::new(::arrow2::array::MutableBooleanArray::from(v)))
            }
        }

        impl From<Vec<Option<bool>>> for $crate::FxVector {
            fn from(vec: Vec<Option<bool>>) -> Self {
                Self(Box::new(::arrow2::array::MutableBooleanArray::from(vec)))
            }
        }

        impl FromSlice<bool, $crate::FxVector> for $crate::FxVector {
            fn from_slice(slice: &[bool]) -> Self {
                Self(Box::new(::arrow2::array::MutableBooleanArray::from_slice(
                    slice,
                )))
            }
        }
    };
}

pub(crate) use arr_impl_from_bool;
pub(crate) use vec_impl_from_bool;

// ================================================================================================
// FxVector
// ================================================================================================

macro_rules! vec_push_branch {
    ($s:ident, $v:expr, $dwn_cst_t:ty, $dwn_cst_m:ident) => {{
        let val = ($v as &dyn ::std::any::Any)
            .downcast_ref::<$dwn_cst_t>()
            .ok_or_else(|| $crate::FxError::InvalidCasting("Invalid type".to_string()))?
            .to_owned();

        $s.0.as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .expect("expect downcast array success")
            .try_push(Some(val))?;

        Ok($s)
    }};
}

macro_rules! vec_pop_branch {
    ($s:ident, $dwn_cst_m:ident) => {{
        $s.0.as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or_else(|| $crate::FxError::InvalidCasting("Invalid type".to_string()))?
            .pop();

        Ok($s)
    }};
    ($s:ident, $dwn_cst_m:ident, $fx_v:ident) => {{
        let res =
            $s.0.as_mut_any()
                .downcast_mut::<$dwn_cst_m>()
                .ok_or_else(|| $crate::FxError::InvalidCasting("Invalid type".to_string()))?
                .pop()
                .ok_or_else(|| $crate::FxError::InvalidOperation("Empty vector".to_string()))?;

        Ok(FxValue::$fx_v(res))
    }};
}

pub(crate) use vec_pop_branch;
pub(crate) use vec_push_branch;

// ================================================================================================
// Connector macros
// ================================================================================================

macro_rules! impl_sql_meta {
    ($db:ident, $row:ident, $db_pool_options:ident, $db_pool:ident) => {
        impl $crate::SqlMeta for ::sqlx::Pool<$db> {
            type FutSelf<'a> = impl ::std::future::Future<Output = FxResult<Self>> + 'a;
            type FutNil<'a> = impl ::std::future::Future<Output = FxResult<()>> + 'a;
            type DB = $db;
            type Row = $row;

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
                pipe: $crate::PipeFn<<Self::DB as ::sqlx::Database>::Row, T>,
            ) -> ::futures::future::BoxFuture<'a, $crate::FxResult<Vec<T>>> {
                let q = async move {
                    Ok(::sqlx::query(sql)
                        .try_map(|r| Ok(pipe(r)))
                        .fetch_all(self)
                        .await?)
                    .and_then(|r| r.into_iter().collect::<$crate::FxResult<Vec<T>>>())
                };
                Box::pin(q)
            }

            fn query_one<'a, T: Send + Unpin + 'a>(
                &'a self,
                sql: &'a str,
                pipe: $crate::PipeFn<<Self::DB as ::sqlx::Database>::Row, T>,
            ) -> ::futures::future::BoxFuture<'a, $crate::FxResult<T>> {
                let q = async move {
                    Ok(::sqlx::query(sql)
                        .try_map(|r| Ok(pipe(r)))
                        .fetch_one(self)
                        .await?)
                    .and_then(|r| r)
                };
                Box::pin(q)
            }

            fn query_as<
                'a,
                T: Send + Unpin + for<'r> ::sqlx::FromRow<'r, <Self::DB as ::sqlx::Database>::Row>,
            >(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<'a, $crate::FxResult<Vec<T>>> {
                let q = async move { Ok(::sqlx::query_as::<_, T>(sql).fetch_all(self).await?) };
                Box::pin(q)
            }

            fn query_one_as<
                'a,
                T: Send + Unpin + for<'r> ::sqlx::FromRow<'r, <Self::DB as ::sqlx::Database>::Row>,
            >(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<'a, FxResult<T>> {
                let q = async move { Ok(::sqlx::query_as::<_, T>(sql).fetch_one(self).await?) };
                Box::pin(q)
            }

            fn query_datagrid<'a, D>(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<'a, FxResult<Datagrid>>
            where
                D: Send + $crate::FxDatagrid,
                D: From<Self::Row>,
            {
                let q = async move {
                    let mut build = D::gen_row_builder();

                    let mut rows = ::sqlx::query(sql).fetch(self);

                    while let Some(row) = rows.try_next().await? {
                        build.stack(row.into());
                    }

                    build.build()
                };

                Box::pin(q)
            }

            fn execute<'a>(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<
                'a,
                $crate::FxResult<<Self::DB as ::sqlx::Database>::QueryResult>,
            > {
                let q = async move { Ok(::sqlx::query(sql).execute(self).await?) };
                Box::pin(q)
            }
        }
    };
}

pub(crate) use impl_sql_meta;
