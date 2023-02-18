//! file: macros.rs
//! author: Jacob Xie
//! date: 2023/01/13 22:42:09 Friday
//! brief: Macros

// ================================================================================================
// utils
// ================================================================================================

macro_rules! invalid_arg {
    ($s:expr, $i:expr) => {
        if $i >= $s.len() {
            return Err($crate::FxError::InvalidArgument(format!(
                "n: {} is greater than array length: {}",
                $i,
                $s.len()
            )));
        }
    };
}

macro_rules! invalid_len {
    ($v:expr) => {
        let iter = $v.iter().map(|a| a.len());
        let lens = ::std::collections::HashSet::<_>::from_iter(iter);
        if lens.len() != 1 {
            return Err($crate::FxError::InvalidArgument(format!(
                "Vector of FxArray have different length: {:?}",
                lens
            )));
        }
    };
}

pub(crate) use invalid_arg;
pub(crate) use invalid_len;

// ================================================================================================
// FxValue
// ================================================================================================

macro_rules! impl_from_x_for_value {
    ($t:ty, $fxv:ident) => {
        impl From<$t> for $crate::FxValue {
            fn from(value: $t) -> Self {
                FxValue::$fxv(value)
            }
        }
    };
}

pub(crate) use impl_from_x_for_value;

// ================================================================================================
// impl from native
// ================================================================================================

macro_rules! arc_arr_impl_from_native {
    ($t:ty) => {
        impl $crate::FromVec<$t, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<$t>) -> $crate::types::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::PrimitiveArray::from(v).arced()
            }
        }

        impl $crate::FromVec<Option<$t>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::types::ArcArr {
                ::arrow2::array::PrimitiveArray::from(vec).arced()
            }
        }

        impl $crate::FromSlice<$t, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[$t]) -> $crate::types::ArcArr {
                ::arrow2::array::PrimitiveArray::from_slice(slice).arced()
            }
        }

        impl $crate::FromSlice<Option<$t>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[Option<$t>]) -> $crate::types::ArcArr {
                let vec = slice.to_vec();
                ::arrow2::array::PrimitiveArray::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_native {
    ($t:ty) => {
        impl $crate::FromVec<$t, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<$t>) -> $crate::types::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let mut v = ::arrow2::array::MutablePrimitiveArray::from(v);
                // since `$t` is never `None`, use `set_validity` to clear incorrect validity info
                v.set_validity(None);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::FromVec<Option<$t>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutablePrimitiveArray::from(vec))
            }
        }

        impl $crate::FromSlice<$t, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[$t]) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutablePrimitiveArray::from_slice(slice))
            }
        }

        impl $crate::FromSlice<Option<$t>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[Option<$t>]) -> $crate::types::ArcVec {
                let vec = slice.to_vec();
                ::std::sync::Arc::new(::arrow2::array::MutablePrimitiveArray::from(vec))
            }
        }
    };
}

pub(crate) use arc_arr_impl_from_native;
pub(crate) use arc_vec_impl_from_native;

// ================================================================================================
// impl from str
// ================================================================================================

macro_rules! arc_arr_impl_from_str {
    ($t:ty) => {
        impl $crate::FromVec<$t, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<$t>) -> $crate::types::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::Utf8Array::<i32>::from(v).arced()
            }
        }

        impl $crate::FromVec<Option<$t>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::types::ArcArr {
                ::arrow2::array::Utf8Array::<i32>::from(vec).arced()
            }
        }

        impl $crate::FromSlice<$t, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[$t]) -> $crate::types::ArcArr {
                ::arrow2::array::Utf8Array::<i32>::from_slice(slice).arced()
            }
        }

        impl $crate::FromSlice<Option<$t>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[Option<$t>]) -> $crate::types::ArcArr {
                let vec = slice.to_vec();
                ::arrow2::array::Utf8Array::<i32>::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_str {
    ($t:ty) => {
        impl $crate::FromVec<$t, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<$t>) -> $crate::types::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let v = ::arrow2::array::MutableUtf8Array::<i32>::from(v);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::FromVec<Option<$t>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableUtf8Array::<i32>::from(vec))
            }
        }

        impl $crate::FromSlice<$t, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[$t]) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableUtf8Array::<i32>::from_iter_values(
                    slice.into_iter(),
                ))
            }
        }

        impl $crate::FromSlice<Option<$t>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[Option<$t>]) -> $crate::types::ArcVec {
                let vec = slice.to_vec();
                ::std::sync::Arc::new(::arrow2::array::MutableUtf8Array::<i32>::from(vec))
            }
        }
    };
}

pub(crate) use arc_arr_impl_from_str;
pub(crate) use arc_vec_impl_from_str;

// ================================================================================================
// impl from bool
// ================================================================================================

macro_rules! arc_arr_impl_from_bool {
    () => {
        impl $crate::FromVec<bool, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<bool>) -> $crate::types::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::BooleanArray::from(v).arced()
            }
        }

        impl $crate::FromVec<Option<bool>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_vec(vec: Vec<Option<bool>>) -> $crate::types::ArcArr {
                ::arrow2::array::BooleanArray::from(vec).arced()
            }
        }

        impl $crate::FromSlice<bool, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[bool]) -> $crate::types::ArcArr {
                ::arrow2::array::BooleanArray::from_slice(slice).arced()
            }
        }

        impl $crate::FromSlice<Option<bool>, $crate::types::ArcArr> for $crate::types::ArcArr {
            fn from_slice(slice: &[Option<bool>]) -> $crate::types::ArcArr {
                let vec = slice.to_vec();
                ::arrow2::array::BooleanArray::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_bool {
    () => {
        impl $crate::FromVec<bool, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<bool>) -> $crate::types::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let v = ::arrow2::array::MutableBooleanArray::from(v);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::FromVec<Option<bool>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_vec(vec: Vec<Option<bool>>) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from(vec))
            }
        }

        impl $crate::FromSlice<bool, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[bool]) -> $crate::types::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from_slice(slice))
            }
        }

        impl $crate::FromSlice<Option<bool>, $crate::types::ArcVec> for $crate::types::ArcVec {
            fn from_slice(slice: &[Option<bool>]) -> $crate::types::ArcVec {
                let vec = slice.to_vec();
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from(vec))
            }
        }
    };
}

pub(crate) use arc_arr_impl_from_bool;
pub(crate) use arc_vec_impl_from_bool;

// ================================================================================================
// FxVector
// ================================================================================================

macro_rules! vec_push_branch {
    ($s:ident, $v:expr, $dwn_cst_t:ty, $dwn_cst_m:ident) => {{
        let val = ($v as &dyn ::std::any::Any)
            .downcast_ref::<$dwn_cst_t>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .to_owned();

        ::std::sync::Arc::get_mut(&mut $s.0)
            .ok_or($crate::FxError::FailedToConvert)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .try_push(Some(val))?;

        Ok($s)
    }};
}

macro_rules! vec_pop_branch {
    ($s:ident, $dwn_cst_m:ident) => {{
        ::std::sync::Arc::get_mut(&mut $s.0)
            .ok_or($crate::FxError::FailedToConvert)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .pop();

        Ok($s)
    }};
    ($s:ident, $dwn_cst_m:ident, $fx_v:ident) => {{
        ::std::sync::Arc::get_mut(&mut $s.0)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()?
            .pop()
            .map($crate::FxValue::$fx_v)
    }};
}

macro_rules! vec_reserve_branch {
    ($s:ident, $dwn_cst_m:ident, $a:expr) => {{
        ::std::sync::Arc::get_mut(&mut $s.0)
            .ok_or($crate::FxError::FailedToConvert)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .reserve($a);

        Ok(())
    }};
    ($s:ident, $dwn_cst_m:ident, $a:expr, $av:expr) => {{
        ::std::sync::Arc::get_mut(&mut $s.0)
            .ok_or($crate::FxError::FailedToConvert)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .reserve($a, $av);

        Ok(())
    }};
}

macro_rules! vec_extend_branch {
    ($s:ident, $v:expr, $dwn_cst_m:ident) => {{
        let iter =
            $v.0.as_any()
                .downcast_ref::<$dwn_cst_m>()
                .ok_or($crate::FxError::InvalidDowncast)?
                .iter();

        ::std::sync::Arc::get_mut(&mut $s.0)
            .ok_or($crate::FxError::FailedToConvert)?
            .as_mut_any()
            .downcast_mut::<$dwn_cst_m>()
            .ok_or($crate::FxError::InvalidDowncast)?
            .extend_trusted_len(iter);

        Ok($s)
    }};
}

pub(crate) use vec_extend_branch;
pub(crate) use vec_pop_branch;
pub(crate) use vec_push_branch;
pub(crate) use vec_reserve_branch;

// ================================================================================================
// Convertion from FxArray to FxVector
// ================================================================================================

macro_rules! arr_to_vec_branch {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr
            .0
            .as_any()
            .downcast_ref::<$dwn_cst_r>()
            .ok_or($crate::FxError::FailedToConvert)?
            .into_iter();

        let mba = $arrow_ma::from_iter(arr);

        Ok($crate::FxVector(::std::sync::Arc::new(mba)))
    }};
}

macro_rules! arr_to_vec_p_branch {
    ($arr:expr, $dwn_cst_r:ident, $arrow_ma:ident) => {{
        let arr = $arr
            .0
            .as_any()
            .downcast_ref::<$dwn_cst_r>()
            .ok_or($crate::FxError::FailedToConvert)?
            .into_iter();

        let mba = $arrow_ma::from_trusted_len_iter(arr);

        Ok($crate::FxVector(::std::sync::Arc::new(mba)))
    }};
}

pub(crate) use arr_to_vec_branch;
pub(crate) use arr_to_vec_p_branch;

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

            fn query_grid<'a, D>(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<'a, FxResult<FxGrid>>
            where
                D: Send + $crate::cont::ab::FxChunkingRowBuilderGenerator<FxGrid>,
                D: From<Self::Row>,
            {
                let q = async move {
                    let mut build = D::gen_chunking_row_builder();

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
