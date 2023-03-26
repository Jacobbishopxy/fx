//! file: macros.rs
//! author: Jacob Xie
//! date: 2023/01/13 22:42:09 Friday
//! brief: Macros

// ================================================================================================
// utils
// ================================================================================================

// macro_rules! invalid_arg {
//     ($s:expr, $i:expr) => {
//         if $i >= $s.len() {
//             return Err($crate::FxError::InvalidArgument(format!(
//                 "n: {} is greater than array length: {}",
//                 $i,
//                 $s.len()
//             )));
//         }
//     };
// }

// macro_rules! invalid_len {
//     ($v:expr) => {
//         let iter = $v.iter().map(|a| a.len());
//         let lens = ::std::collections::HashSet::<_>::from_iter(iter);
//         if lens.len() != 1 {
//             return Err($crate::FxError::InvalidArgument(format!(
//                 "Vector of FxArray have different length: {:?}",
//                 lens
//             )));
//         }
//     };
// }

// pub(crate) use invalid_arg;
// pub(crate) use invalid_len;

// ================================================================================================
// FxValue
// ================================================================================================

macro_rules! impl_from_x_for_value {
    ($t:ty, $fxv:ident) => {
        impl From<$t> for $crate::value::FxValue {
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
        impl $crate::ab::FromVec<$t, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<$t>) -> $crate::cont::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::PrimitiveArray::from(v).arced()
            }
        }

        impl $crate::ab::FromVec<Option<$t>, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::cont::ArcArr {
                ::arrow2::array::PrimitiveArray::from(vec).arced()
            }
        }

        impl<S: AsRef<[$t]>> $crate::ab::FromSlice<S, [$t], $crate::cont::ArcArr>
            for $crate::cont::ArcArr
        {
            fn from_slice(slice: S) -> $crate::cont::ArcArr {
                ::arrow2::array::PrimitiveArray::from_slice(slice.as_ref()).arced()
            }
        }

        impl<S: AsRef<[Option<$t>]>> $crate::ab::FromSlice<S, [Option<$t>], $crate::cont::ArcArr>
            for $crate::cont::ArcArr
        {
            fn from_slice(slice: S) -> $crate::cont::ArcArr {
                let vec = slice.as_ref().to_vec();
                ::arrow2::array::PrimitiveArray::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_native {
    ($t:ty) => {
        impl $crate::ab::FromVec<$t, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<$t>) -> $crate::cont::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let mut v = ::arrow2::array::MutablePrimitiveArray::from(v);
                // since `$t` is never `None`, use `set_validity` to clear incorrect validity info
                v.set_validity(None);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::ab::FromVec<Option<$t>, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::cont::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutablePrimitiveArray::from(vec))
            }
        }

        impl<S: AsRef<[$t]>> $crate::ab::FromSlice<S, [$t], $crate::cont::ArcVec>
            for $crate::cont::ArcVec
        {
            fn from_slice(slice: S) -> $crate::cont::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutablePrimitiveArray::from_slice(
                    slice.as_ref(),
                ))
            }
        }

        impl<S: AsRef<[Option<$t>]>> $crate::ab::FromSlice<S, [Option<$t>], $crate::cont::ArcVec>
            for $crate::cont::ArcVec
        {
            fn from_slice(slice: S) -> $crate::cont::ArcVec {
                let vec = slice.as_ref().to_vec();
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
        impl $crate::ab::FromVec<$t, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<$t>) -> $crate::cont::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::Utf8Array::<i32>::from(v).arced()
            }
        }

        impl $crate::ab::FromVec<Option<$t>, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::cont::ArcArr {
                ::arrow2::array::Utf8Array::<i32>::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_str {
    ($t:ty) => {
        impl $crate::ab::FromVec<$t, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<$t>) -> $crate::cont::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let v = ::arrow2::array::MutableUtf8Array::<i32>::from(v);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::ab::FromVec<Option<$t>, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<Option<$t>>) -> $crate::cont::ArcVec {
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
        impl $crate::ab::FromVec<bool, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<bool>) -> $crate::cont::ArcArr {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                ::arrow2::array::BooleanArray::from(v).arced()
            }
        }

        impl $crate::ab::FromVec<Option<bool>, $crate::cont::ArcArr> for $crate::cont::ArcArr {
            fn from_vec(vec: Vec<Option<bool>>) -> $crate::cont::ArcArr {
                ::arrow2::array::BooleanArray::from(vec).arced()
            }
        }

        impl<S: AsRef<[bool]>> $crate::ab::FromSlice<S, [bool], $crate::cont::ArcArr>
            for $crate::cont::ArcArr
        {
            fn from_slice(slice: S) -> $crate::cont::ArcArr {
                ::arrow2::array::BooleanArray::from_slice(slice.as_ref()).arced()
            }
        }

        impl<S: AsRef<[Option<bool>]>>
            $crate::ab::FromSlice<S, [Option<bool>], $crate::cont::ArcArr>
            for $crate::cont::ArcArr
        {
            fn from_slice(slice: S) -> $crate::cont::ArcArr {
                let vec = slice.as_ref().to_vec();
                ::arrow2::array::BooleanArray::from(vec).arced()
            }
        }
    };
}

macro_rules! arc_vec_impl_from_bool {
    () => {
        impl $crate::ab::FromVec<bool, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<bool>) -> $crate::cont::ArcVec {
                let v = vec.into_iter().map(Option::from).collect::<Vec<_>>();
                let v = ::arrow2::array::MutableBooleanArray::from(v);
                ::std::sync::Arc::new(v)
            }
        }

        impl $crate::ab::FromVec<Option<bool>, $crate::cont::ArcVec> for $crate::cont::ArcVec {
            fn from_vec(vec: Vec<Option<bool>>) -> $crate::cont::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from(vec))
            }
        }

        impl<S: AsRef<[bool]>> $crate::ab::FromSlice<S, [bool], $crate::cont::ArcVec>
            for $crate::cont::ArcVec
        {
            fn from_slice(slice: S) -> $crate::cont::ArcVec {
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from_slice(slice))
            }
        }

        impl<S: AsRef<[Option<bool>]>>
            $crate::ab::FromSlice<S, [Option<bool>], $crate::cont::ArcVec>
            for $crate::cont::ArcVec
        {
            fn from_slice(slice: S) -> $crate::cont::ArcVec {
                let vec = slice.as_ref().to_vec();
                ::std::sync::Arc::new(::arrow2::array::MutableBooleanArray::from(vec))
            }
        }
    };
}

pub(crate) use arc_arr_impl_from_bool;
pub(crate) use arc_vec_impl_from_bool;

// ================================================================================================
// Connector macros
// ================================================================================================

macro_rules! impl_sql_meta {
    ($db:ident, $row:ident, $db_pool_options:ident, $db_pool:ident) => {
        impl $crate::io::SqlMeta for ::sqlx::Pool<$db> {
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
                pipe: $crate::io::PipeFn<<Self::DB as ::sqlx::Database>::Row, T>,
            ) -> ::futures::future::BoxFuture<'a, $crate::error::FxResult<Vec<T>>> {
                let q = async move {
                    Ok(::sqlx::query(sql)
                        .try_map(|r| Ok(pipe(r)))
                        .fetch_all(self)
                        .await?)
                    .and_then(|r| r.into_iter().collect::<$crate::error::FxResult<Vec<T>>>())
                };
                Box::pin(q)
            }

            fn query_one<'a, T: Send + Unpin + 'a>(
                &'a self,
                sql: &'a str,
                pipe: $crate::io::PipeFn<<Self::DB as ::sqlx::Database>::Row, T>,
            ) -> ::futures::future::BoxFuture<'a, $crate::error::FxResult<T>> {
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
            ) -> ::futures::future::BoxFuture<'a, $crate::error::FxResult<Vec<T>>> {
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

            // fn query_grid<'a, D>(
            //     &'a self,
            //     sql: &'a str,
            // ) -> ::futures::future::BoxFuture<'a, FxResult<FxGrid>>
            // where
            //     D: Send + $crate::cont::FxChunkingRowBuilderGenerator<FxGrid>,
            //     D: From<Self::Row>,
            // {
            //     let q = async move {
            //         let mut build = D::gen_chunking_row_builder();

            //         let mut rows = ::sqlx::query(sql).fetch(self);

            //         while let Some(row) = rows.try_next().await? {
            //             build.stack(row.into());
            //         }

            //         build.build()
            //     };

            //     Box::pin(q)
            // }

            fn execute<'a>(
                &'a self,
                sql: &'a str,
            ) -> ::futures::future::BoxFuture<
                'a,
                $crate::error::FxResult<<Self::DB as ::sqlx::Database>::QueryResult>,
            > {
                let q = async move { Ok(::sqlx::query(sql).execute(self).await?) };
                Box::pin(q)
            }
        }
    };
}

pub(crate) use impl_sql_meta;
