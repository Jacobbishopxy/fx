//! file: sql_impl.rs
//! author: Jacob Xie
//! date: 2023/03/15 20:38:41 Wednesday
//! brief:

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

use crate::helper::*;

// ================================================================================================
// Sql related Impl
// ================================================================================================

/// io: sql
pub(crate) fn gen_impl_from_sql_row(
    struct_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let ctt = named_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let idx = syn::Index::from(i);
            let fd = f.ident.as_ref().unwrap();
            quote! { #fd: v.get(#idx) }
        })
        .collect::<Vec<_>>();

    quote! {
        use ::sqlx::Row;


        impl From<::sqlx::mssql::MssqlRow> for #struct_name {
            fn from(v: ::sqlx::mssql::MssqlRow) -> Self {
                Self {
                    #(#ctt),*
                }
            }
        }

        impl From<::sqlx::mysql::MySqlRow> for #struct_name {
            fn from(v: ::sqlx::mysql::MySqlRow) -> Self {
                Self {
                    #(#ctt),*
                }
            }
        }

        impl From<::sqlx::postgres::PgRow> for #struct_name {
            fn from(v: ::sqlx::postgres::PgRow) -> Self {
                Self {
                    #(#ctt),*
                }
            }
        }
    }
}
