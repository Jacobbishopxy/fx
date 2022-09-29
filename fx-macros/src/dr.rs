//! Derive

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    punctuated::Punctuated, token::Comma, Attribute, Data, DeriveInput, Field, Fields, Ident, Lit,
    Meta, NestedMeta, Type, TypeTuple,
};

type NamedFields = Punctuated<Field, Comma>;

const UE: &str = "fx only supports Struct and is not implemented for Enum";
const UU: &str = "fx only supports Struct and is not implemented for Union";

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
        Data::Enum(_) => unimplemented!("{}", UE),
        Data::Union(_) => unimplemented!("{}", UU),
    }
}

fn schema_len(named_fields: &NamedFields) -> usize {
    named_fields.len()
}

fn schema_types(named_fields: &NamedFields) -> TokenStream {
    let fields = named_fields
        .iter()
        .map(|f| {
            let ty = &f.ty;
            quote! { Vec::<#ty>::new() }
        })
        .collect::<Vec<_>>();

    quote! {
        (#(#fields),*)
    }
}

fn generated_schema(named_fields: &NamedFields) -> TokenStream {
    let fields = named_fields
        .iter()
        .map(|f| {
            let ty = &f.ty;

            todo!()
        })
        .collect::<Vec<_>>();

    todo!()
}

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = named_fields(input);

    let schema_len = schema_len(&named_fields);
    let types_tuple = schema_types(&named_fields);
    let schema = generated_schema(&named_fields);

    let expanded = quote! {
        impl FxDatagridTypedRowBuild<#schema_len> for #name {
            fn build(builder: DatagridRowWiseBuilder<#schema_len>) -> crate::FxResult<crate::Datagrid> {
                let mut buck = #types_tuple;

                todo!()
            }

            fn schema() -> crate::FxSchema<#schema_len> {
                let schema = #schema;

                todo!()
            }
        }
    };

    expanded
}
