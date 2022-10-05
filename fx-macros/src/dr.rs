//! Derive

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Data, DeriveInput, Field, Fields, Ident, Type};

type NamedFields = Punctuated<Field, Comma>;

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

#[allow(dead_code)]
fn schema_len(named_fields: &NamedFields) -> usize {
    named_fields.len()
}

#[allow(dead_code)]
fn path_is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(tp) => {
            let path = &tp.path;
            tp.qself.is_none()
                && path.leading_colon.is_none()
                && path.segments.len() == 1
                && path.segments.iter().next().unwrap().ident == "Option"
        }
        _ => panic!("type mismatch"),
    }
}

#[allow(dead_code)]
fn get_option_type(ty: &Type) -> (bool, Ident) {
    match ty {
        Type::Path(tp) => {
            let path = &tp.path;
            let is_option = tp.qself.is_none()
                && path.leading_colon.is_none()
                && path.segments.len() == 1
                && path.segments.iter().next().unwrap().ident == "Option";

            if is_option {
                match &path.segments.first().unwrap().arguments {
                    syn::PathArguments::AngleBracketed(ab) => {
                        let ga = ab.args.first().unwrap();
                        match ga {
                            syn::GenericArgument::Type(Type::Path(t)) => {
                                (true, t.path.segments.first().unwrap().ident.clone())
                            }
                            _ => panic!("type mismatch"),
                        }
                    }
                    _ => panic!("type mismatch"),
                }
            } else {
                (false, path.segments.first().unwrap().ident.clone())
            }
        }
        _ => panic!("type mismatch"),
    }
}

fn generated_build_name(struct_name: &Ident) -> Ident {
    format_ident!("{}RowBuild", struct_name)
}

fn generated_impl_from_sql_row(struct_name: &Ident, named_fields: &NamedFields) -> TokenStream {
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

fn generated_new_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
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

        impl crate::datagrid::FxDatagridRowBuilderCst for #build_name {
            fn new() -> Self {
                Self::default()
            }
        }
    }
}

fn generated_impl_row_build(
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
        impl crate::datagrid::FxDatagridRowBuilder<#struct_name> for #build_name {
            fn stack(&mut self, row: #struct_name) {
                #(#stack_ctt);*;
            }

            fn build(self: ::std::boxed::Box<Self>) -> crate::error::FxResult<crate::datagrid::Datagrid> {
                crate::datagrid::Datagrid::try_from(vec![
                    #(#build_ctt),*
                ])
            }
        }

        impl crate::datagrid::FxDatagrid for #struct_name {
            fn gen_row_builder() -> ::std::boxed::Box<dyn crate::datagrid::FxDatagridRowBuilder<Self>> {
                ::std::boxed::Box::new(#build_name::new())
            }
        }
    }
}

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let struct_name = input.ident.clone();
    let build_name = generated_build_name(&struct_name);
    let named_fields = named_fields(input);

    // auto generated code
    let impl_from_sql_row = generated_impl_from_sql_row(&struct_name, &named_fields);
    let builder_struct = generated_new_builder_struct(&build_name, &named_fields);
    let impl_row_build = generated_impl_row_build(&struct_name, &build_name, &named_fields);

    let expanded = quote! {

        #impl_from_sql_row

        #builder_struct

        #impl_row_build
    };

    expanded
}
