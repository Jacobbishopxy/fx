//! Derive

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Data, DeriveInput, Field, Fields, Ident};

type NamedFields = Punctuated<Field, Comma>;

// ================================================================================================
// Helper Functions
// ================================================================================================

/// turn ast into `Punctuated<Field, Comma>`, and filter out any type that is not a Rust struct
fn named_fields(ast: &DeriveInput) -> NamedFields {
    match &ast.data {
        Data::Struct(s) => {
            if let Fields::Named(ref named_fields) = s.fields {
                named_fields.named.clone()
            } else {
                unimplemented!("derive(Builder) only supports named fields")
            }
        }
        _ => unimplemented!("fx only supports Struct and is not implemented for Enum/Union"),
    }
}

fn gen_chunking_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}ChunkingBuild", struct_name)
}

fn gen_container_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}ContainerBuild", struct_name)
}

// ================================================================================================
// Sql related Impl
// ================================================================================================

// TODO: generic container

fn gen_impl_from_sql_row(struct_name: &Ident, named_fields: &NamedFields) -> TokenStream {
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

// ================================================================================================
// Chunking builder
// ================================================================================================

fn gen_chunking_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
    let ctt = named_fields
        .iter()
        .map(|f| {
            let fd = f.ident.as_ref().unwrap();
            let ty = &f.ty;

            quote! { #fd: Vec<#ty> }
        })
        .collect::<Vec<_>>();

    quote! {
        #[derive(Default)]
        struct #build_name { #(#ctt),* }
    }
}

fn gen_impl_chunking(
    struct_name: &Ident,
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let (stack_ctt, build_ctt): (Vec<_>, Vec<_>) = named_fields
        .iter()
        .map(|f| {
            let fd = f.ident.as_ref().unwrap();

            let sc = quote! {
                self.#fd.push(row.#fd)
            };
            let bc = quote! {
                crate::FxArray::from(self.#fd)
            };

            (sc, bc)
        })
        .unzip();

    quote! {
        impl crate::FxChunkingRowBuilder<#struct_name,crate::FxGrid> for #build_name {
            fn new() -> Self {
                Self::default()
            }

            fn stack(&mut self, row: #struct_name)-> &mut Self {
                #(#stack_ctt);*;

                self
            }

            fn build(self) -> crate::error::FxResult<crate::FxGrid> {
                crate::FxGrid::try_from(vec![
                    #(#build_ctt),*
                ])
            }
        }

        impl crate::FxChunkingRowBuilderGenerator<crate::FxGrid> for #struct_name {
            type Builder = #build_name;

            fn gen_chunking_row_builder() -> Self::Builder {
                #build_name::new()
            }
        }
    }
}

// ================================================================================================
// Container builder
// ================================================================================================

fn gen_container_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
    todo!()
}

fn gen_impl_container(
    struct_name: &Ident,
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    todo!()
}

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let struct_name = input.ident.clone();
    let named_fields = named_fields(input);

    // auto generated code (chunking)
    let chunking_name = gen_chunking_build_name(&struct_name);
    let impl_from_sql_row = gen_impl_from_sql_row(&struct_name, &named_fields);
    let chunking_builder_struct = gen_chunking_builder_struct(&chunking_name, &named_fields);
    let impl_chunking_row_build = gen_impl_chunking(&struct_name, &chunking_name, &named_fields);

    // auto generated code (container)
    // let container_name = gen_container_build_name(&struct_name);
    // let container_builder_struct = gen_container_builder_struct(&container_name, &named_fields);
    // let impl_container_row_build = gen_impl_container(&struct_name, &chunking_name, &named_fields);

    let expanded = quote! {

        #impl_from_sql_row

        #chunking_builder_struct

        #impl_chunking_row_build

        // #container_name

        // #container_builder_struct

        // #impl_container_row_build
    };

    expanded
}
