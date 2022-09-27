//! Derive

use quote::quote;
use syn::{
    punctuated::Punctuated, token::Comma, Attribute, Data, DeriveInput, Field, Fields, Ident, Lit,
    Meta, NestedMeta,
};

pub(crate) fn impl_fx(input: &DeriveInput) -> proc_macro2::TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = name_fields(input);

    let expanded = quote! {
        // TODO
    };

    expanded
}
