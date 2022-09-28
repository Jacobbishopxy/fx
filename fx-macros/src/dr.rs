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

fn schema_types(named_fields: &NamedFields) -> String {
    let mut types_ts = String::new();

    for f in named_fields.iter() {
        // let t = f.ty;
        let t = f.ident.as_ref().unwrap().to_string();
        types_ts += &t;
    }

    // quote! {
    //    #(#types_ts)*
    // }

    types_ts
}

pub(crate) fn impl_fx(input: &DeriveInput) -> proc_macro2::TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = named_fields(input);

    let schema_len = schema_len(&named_fields);
    let names = schema_types(&named_fields);

    let expanded = quote! {
        impl FxDatagridTypedRowBuild<#schema_len> for #name {
            fn build(builder: DatagridRowWiseBuilder<#schema_len>) -> crate::FxResult<crate::Datagrid> {
                // let mut buck =
                todo!()
            }

            fn dev() -> String {
                #names.to_string()
            }
        }
    };

    expanded
}
