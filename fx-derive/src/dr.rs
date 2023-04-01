//! file: dr.rs
//! author: Jacob Xie
//! date: 2023/02/10 21:10:37 Friday
//! brief: Derive
//!
//! There are three markers can be chose while using the `FX` proc-macro (see Constants).
//! These markers are used for distinguishing which kind of `Eclectic` and `Receptacle` to
//! be constructed.
//!
//! We can choose one from below (default "batch"):
//! 1. "chunk": `ChunkArr` as the Eclectic type, and `Vec<ChunkArr>` as the Receptacle type;
//! 2. "batch": `FxBatch` as the Eclectic type, and `FxBatchs<ChunkArr>` as the Receptacle type;
//! 3. "bundle": `FxBundle` as the Eclectic type, and `FxBundles` as the Receptacle type.
//! 4. "table": 1, 2, and 3 as the Eclectic type, and `FxTable` as the Receptacle type.
//! 5. "tabular": 1, 2, and 3 as the Eclectic type, and `FxTabular` as the Receptacle type.
//!
//! Note that `ARRAA` works for constructing `FxBundle` and `FxBundles`, and since we already have "chunk"
//! option, which behaves pretty much the same as the "arraa" and has a stronger restriction (same length),
//! there is no need to provide an extra "arraa" builder.

use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

use crate::constant::*;
use crate::eclectic_builder::*;
use crate::helper::*;
use crate::receptacle_builder::*;
use crate::sql_impl::*;

// ================================================================================================
// Main impl
// ================================================================================================

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let struct_name = input.ident.clone();
    let named_fields = named_fields(input);
    let schema_len = schema_len(&named_fields);

    // get the first attribute from "fx", default to BATCH
    let e_type = get_first_attribute(input, "fx")
        .and_then(filter_attributes)
        .unwrap_or(BATCH.to_owned());

    // auto generated code (eclectic)
    let eclectic_build_name = gen_eclectic_build_name(&struct_name);
    let impl_from_sql_row = gen_impl_from_sql_row(&struct_name, &named_fields);
    let eclectic_builder_struct = gen_eclectic_builder_struct(&eclectic_build_name, &named_fields);
    let impl_eclectic_row_build = gen_multiple_impl_eclectic(
        &e_type,
        schema_len,
        &struct_name,
        &eclectic_build_name,
        &named_fields,
    );

    // auto generated code (container)
    let container_build_name = gen_container_build_name(&struct_name);
    let container_builder_struct = gen_collection_builder_struct(
        &e_type,
        schema_len,
        &eclectic_build_name,
        &container_build_name,
    );
    let impl_container_row_build = gen_multiple_impl_container(
        &e_type,
        schema_len,
        &struct_name,
        &eclectic_build_name,
        &container_build_name,
        &named_fields,
    );

    let expanded = quote! {
        #impl_from_sql_row

        #eclectic_builder_struct

        #impl_eclectic_row_build

        #container_builder_struct

        #impl_container_row_build
    };

    expanded
}
