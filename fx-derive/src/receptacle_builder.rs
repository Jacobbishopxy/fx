//! file: receptacle_builder.rs
//! author: Jacob Xie
//! date: 2023/03/15 20:34:14 Wednesday
//! brief:

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

use crate::constant::*;
use crate::helper::*;

// ================================================================================================
// Receptacle builder
// ================================================================================================

pub(crate) fn gen_collection_builder_struct(
    collection_e_type: &str,
    schema_len: usize,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
) -> TokenStream {
    match collection_e_type {
        CHUNK => quote! {
            struct #container_build_name<T: Eclectic> {
                result: Vec<ChunkArr>,
                buffer: Option<#eclectic_build_name<ChunkArr>>,
                _e: ::std::marker::PhantomData<T>,
            }
        },
        BATCH => quote! {
            struct #container_build_name<T: Eclectic> {
                result: FxBatches::<ChunkArr>,
                buffer: Option<#eclectic_build_name<ChunkArr>>,
                _e: ::std::marker::PhantomData<T>,
            }
        },
        BUNDLE => quote! {
            struct #container_build_name<T: Eclectic> {
                result: FxBundles::<#schema_len, ArcArr>,
                buffer: Option<#eclectic_build_name<T>>,
                _e: ::std::marker::PhantomData<T>,
            }
        },
        TABLE => quote! {
            struct #container_build_name<T: Eclectic> {
                result: FxTable::<#schema_len>,
                buffer: Option<#eclectic_build_name<T>>,
                _e: ::std::marker::PhantomData<T>,
            }
        },
        TABULAR => quote! {
            struct #container_build_name<T: Eclectic> {
                result: FxTabular,
                buffer: Option<#eclectic_build_name<T>>,
                _e: ::std::marker::PhantomData<T>,
            }
        },
        _ => panic!("Unsupported type"),
    }
}

/// gen buffer struct, used in Bundle(not yet) & Table
fn gen_buffer_struct(struct_name: &Ident, e_type: &str) -> TokenStream {
    match e_type {
        ARRAA => quote! {
            #struct_name::gen_arraa_builder()
        },
        CHUNK => quote! {
            #struct_name::gen_chunk_builder()
        },
        BATCH => quote! {
            #struct_name::gen_batch_builder()
        },
        _ => panic!("Unsupported type"),
    }
}

/// collection types
///
/// return type:
/// 1. has_schema;
/// 2. Eclectic;
/// 3. Receptacle;
/// 4. result & buffer in `fn new()`.
fn gen_collection_type(
    base_builder_e_type: &str,
    collection_e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    named_fields: &NamedFields,
) -> (bool, TokenStream, TokenStream, TokenStream) {
    let fields_ctt = named_fields.iter().map(gen_arrow_field).collect::<Vec<_>>();

    match collection_e_type {
        CHUNK => (
            false,
            quote! { ChunkArr },
            quote! { Vec<ChunkArr> },
            quote! {
                let result = Vec::<ChunkArr>::new();
                let buffer = Some(#struct_name::gen_chunk_builder());
            },
        ),
        BATCH => (
            true,
            quote! { ChunkArr },
            quote! { FxBatches::<ChunkArr> },
            quote! {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                let result = FxBatches::<ChunkArr>::empty_with_schema(schema);
                let buffer = Some(#struct_name::gen_chunk_builder());
            },
        ),
        BUNDLE => (
            true,
            quote! { [ArcArr; #schema_len] },
            quote! { FxBundles::<#schema_len, ArcArr> },
            quote! {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                let result = FxBundles::<#schema_len, ArcArr>::empty_with_schema(schema);
                let buffer = Some(#struct_name::gen_arraa_builder());
            },
        ),
        TABLE => {
            let buffer = gen_buffer_struct(struct_name, base_builder_e_type);
            let marker_type = gen_eclectic_type(schema_len, base_builder_e_type);
            (
                true,
                marker_type,
                quote! { FxTable::<#schema_len> },
                quote! {
                    let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                    let result = FxTable::<#schema_len>::empty_with_schema(schema);
                    let buffer = Some(#buffer);
                },
            )
        }
        TABULAR => {
            let buffer = gen_buffer_struct(struct_name, base_builder_e_type);
            let marker_type = gen_eclectic_type(schema_len, base_builder_e_type);
            (
                true,
                marker_type,
                quote! { FxTabular },
                quote! {
                    let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                    let result = FxTabular::empty_with_schema(schema);
                    let buffer = Some(#buffer);
                },
            )
        }
        _ => panic!("Unsupported type"),
    }
}

/// Generate impl collection builder generator
fn gen_impl_cbg(
    base_builder_e_type: &str,
    collection_e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    collection_build_name: &Ident,
) -> TokenStream {
    match collection_e_type {
        CHUNK => quote! {
            impl FxChunksGenerator for #struct_name {
                type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                type ChunksBuilder = #collection_build_name<ChunkArr>;
            }
        },
        BATCH => quote! {
            impl FxChunkBatchesGenerator for #struct_name {
                type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                type BatchesBuilder = #collection_build_name<ChunkArr>;
            }
        },
        BUNDLE => quote! {
            impl FxArraaBundlesGenerator<#schema_len> for #struct_name {
                type ArraaBuilder = #eclectic_build_name<[ArcArr; #schema_len]>;

                type BundlesBuilder = #collection_build_name<[ArcArr; #schema_len]>;
            }
        },
        TABLE => match base_builder_e_type {
            ARRAA => quote! {
                impl FxArraaTableGenerator<#schema_len> for #struct_name {
                    type ArraaBuilder = #eclectic_build_name<[ArcArr; #schema_len]>;

                    type TableBuilder = #collection_build_name<[ArcArr; #schema_len]>;
                }
            },
            CHUNK => quote! {
                impl FxChunkTableGenerator<#schema_len> for #struct_name {
                    type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                    type TableBuilder = #collection_build_name<ChunkArr>;
                }
            },
            BATCH => quote! {
                impl FxBatchTableGenerator<#schema_len> for #struct_name {
                    type BatchBuilder = #eclectic_build_name<FxBatch>;

                    type TableBuilder = #collection_build_name<FxBatch>;
                }
            },
            _ => panic!("Unsupported type for FxTable"),
        },
        TABULAR => match base_builder_e_type {
            ARRAA => quote! {
                impl FxArraaTabularGenerator<#schema_len> for #struct_name {
                    type ArraaBuilder = #eclectic_build_name<[ArcArr; #schema_len]>;

                    type TabularBuilder = #collection_build_name<[ArcArr; #schema_len]>;
                }
            },
            CHUNK => quote! {
                impl FxChunkTabularGenerator for #struct_name {
                    type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                    type TabularBuilder = #collection_build_name<ChunkArr>;
                }
            },
            BATCH => quote! {
                impl FxBatchTabularGenerator for #struct_name {
                    type BatchBuilder = #eclectic_build_name<FxBatch>;

                    type TabularBuilder = #collection_build_name<FxBatch>;
                }
            },
            _ => panic!("Unsupported type for FxTabular"),
        },
        _ => panic!("Unsupported type"),
    }
}

/// impl container
///
/// VecChunk
/// FxBatches
/// FxBundles
pub(crate) fn gen_impl_container(
    base_builder_e_type: &str,
    collection_e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let (has_schema, eclectic_type, eclectic_collection, result_and_buffer) = gen_collection_type(
        base_builder_e_type,
        collection_e_type,
        schema_len,
        struct_name,
        named_fields,
    );

    let impl_build_gen = gen_impl_cbg(
        base_builder_e_type,
        collection_e_type,
        schema_len,
        struct_name,
        eclectic_build_name,
        container_build_name,
    );

    quote! {
        impl FxCollectionBuilder<
            #has_schema, #eclectic_build_name<#eclectic_type>, #struct_name, #eclectic_collection, usize, #eclectic_type
        > for #container_build_name<#eclectic_type>
        {
            fn new() -> FxResult<Self> {
                #result_and_buffer

                Ok(Self { result, buffer, _e: ::std::marker::PhantomData })
            }

            fn mut_buffer(&mut self) -> Option<&mut #eclectic_build_name<#eclectic_type>> {
                self.buffer.as_mut()
            }

            fn set_buffer(&mut self, buffer: #eclectic_build_name<#eclectic_type>) {
                self.buffer = Some(buffer);
            }

            fn take_buffer(&mut self) -> Option<#eclectic_build_name<#eclectic_type>> {
                self.buffer.take()
            }

            fn mut_result(&mut self) -> &mut #eclectic_collection {
                &mut self.result
            }

            fn take_result(self) -> #eclectic_collection {
                self.result
            }
        }

        #impl_build_gen
    }
}

pub(crate) fn gen_multiple_impl_container(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let impls = match e_type {
        CHUNK => {
            vec![
                //
                gen_impl_container(
                    CHUNK,
                    CHUNK,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
            ]
        }
        BATCH => {
            vec![
                //
                gen_impl_container(
                    CHUNK,
                    BATCH,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
            ]
        }
        BUNDLE => {
            vec![
                // TODO: more options
                gen_impl_container(
                    CHUNK,
                    BUNDLE,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
            ]
        }
        TABLE => {
            vec![
                gen_impl_container(
                    ARRAA,
                    TABLE,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
                gen_impl_container(
                    CHUNK,
                    TABLE,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
                gen_impl_container(
                    BATCH,
                    TABLE,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
            ]
        }
        TABULAR => {
            vec![
                gen_impl_container(
                    ARRAA,
                    TABULAR,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
                gen_impl_container(
                    CHUNK,
                    TABULAR,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
                gen_impl_container(
                    BATCH,
                    TABULAR,
                    schema_len,
                    struct_name,
                    eclectic_build_name,
                    container_build_name,
                    named_fields,
                ),
            ]
        }
        _ => panic!("Unsupported type"),
    };

    quote! {
        #(#impls)*
    }
}
