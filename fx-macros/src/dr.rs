//! Derive

use quote::quote;
use syn::{
    punctuated::Punctuated, token::Comma, Attribute, Data, DeriveInput, Field, Fields, Ident, Lit,
    Meta, NestedMeta,
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

pub(crate) fn impl_fx(input: &DeriveInput) -> proc_macro2::TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = named_fields(input);

    let expanded = quote! {
        // TODO
    };

    expanded
}
