//! file: dr.rs
//! author: Jacob Xie
//! date: 2023/02/10 21:10:37 Friday
//! brief: Derive
//!
//! There are three markers can be chose while using the `FX` proc-macro (see Constants).
//! These markers are used for distinguishing which kind of `Eclectic` and `EclecticCollection` to
//! be constructed.
//!
//! We can choose one from below (default "batch"):
//! 1. "chunk": `ChunkArr` as the Eclectic type, and `Vec<ChunkArr>` as the EclecticCollection type;
//! 2. "batch": `FxBatch` as the Eclectic type, and `FxBatchs<ChunkArr>` as the EclecticCollection type;
//! 3. "table": `FxTable` as the Eclectic type, and `FxTables` as the EclecticCollection type.
//!
//! Note that `ARRAA` works for constructing `FxTable` and `FxTables`, and since we already have "chunk"
//! option, which behaves pretty much the same as the "arraa" and has a stronger restriction (same length),
//! there is no need to provide an extra "arraa" builder.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, Ident};

use crate::helper::*;

// ================================================================================================
// Constants
// ================================================================================================

const CHUNK: &str = "chunk"; // Chunk<Arc<dyn Array>>
const ARRAA: &str = "arraa"; // [Arc<dyn Array>; W]. 'arraa' denotes (Array of ArcArr)
const BATCH: &str = "batch"; // FxBatch
const TABLE: &str = "table"; // FxTable<W; Arc<dyn Array>>

// Note: Array is a trait provided by [arrow](https://github.com/jorgecarleitao/arrow2)

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

/// generate eclectic builder and its generator
fn gen_eclectic_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}EclecticBuild", struct_name)
}

/// generate container builder and its generator
fn gen_container_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}EcLecticContainerBuild", struct_name)
}

/// generate eclectic type by string
fn gen_eclectic_type(schema_len: usize, s: &str) -> TokenStream {
    match s {
        CHUNK => quote! {ChunkArr},
        ARRAA => quote! {[ArcArr; #schema_len]},
        BATCH => quote! {FxBatch},
        TABLE => quote! {FxTable::<#schema_len, ArcArr>},
        _ => quote! {FxBatch}, // default to FxBatch
    }
}

/// generate container type by string
#[allow(dead_code)]
fn gen_container_type(schema_len: usize, s: &str) -> TokenStream {
    match s {
        CHUNK => quote! {Vec<ChunkArr>},
        BATCH => quote! {FxBatches::<ChunkArr>},
        TABLE => quote! {FxTables::<#schema_len, ArcArr>},
        _ => quote! {FxBatches::<ChunkArr>}, // default to FxBatches
    }
}

/// generate arrow's field
fn gen_arrow_field(f: &Field) -> TokenStream {
    let fd = f.ident.as_ref().unwrap().to_string();
    let ty = &f.ty;

    let (is_option, type_name) = get_option_type_name(ty);

    match type_name.as_str() {
        "bool" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Boolean, #is_option)
        },
        "i8" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Int8, #is_option)
        },
        "i16" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Int16, #is_option)
        },
        "i32" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Int32, #is_option)
        },
        "i64" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Int64, #is_option)
        },
        "u8" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::UInt8, #is_option)
        },
        "u16" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::UInt16, #is_option)
        },
        "u32" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::UInt32, #is_option)
        },
        "u64" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::UInt64, #is_option)
        },
        "f32" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Float32, #is_option)
        },
        "f64" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Float64, #is_option)
        },
        "String" => quote! {
            ::arrow2::datatypes::Field::new(#fd, ::arrow2::datatypes::DataType::Utf8, #is_option)
        },
        _ => panic!("unsupported type!"),
    }
}

// ================================================================================================
// Sql related Impl
// ================================================================================================

// TODO: generic container

/// io: sql
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
// Eclectic builder
// ================================================================================================

/// Generate a builder struct for eclectic type. Note, a generic T is given as a phantom type, and
/// this is the simpliest way to destinguish which builder to be used during the genarator's impl,
/// details in [tests/fx_builder_test.rs] the third part.
fn gen_eclectic_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
    let ctt = named_fields
        .iter()
        .map(|f| {
            let fd = f.ident.as_ref().unwrap();
            let ty = &f.ty;

            quote! { #fd: Vec<#ty>, }
        })
        .collect::<Vec<_>>();

    quote! {
        struct #build_name<T: Eclectic> {
            #(#ctt)*
            _e: ::std::marker::PhantomData<T>,
        }
    }
}

/// Generate:
/// 1. field names;
/// 2. self content in `fn new`;
/// 3. pushing behavior in `fn stack`;
/// 4. build behavior in `fn build`.
fn gen_cct(
    named_fields: &NamedFields,
) -> (
    Vec<String>,
    Vec<TokenStream>,
    Vec<TokenStream>,
    Vec<TokenStream>,
) {
    let mut nm = vec![];
    let mut sf = vec![];
    let mut sc = vec![];
    let mut bc = vec![];

    for f in named_fields.iter() {
        let fd = f.ident.as_ref().unwrap();

        nm.push(fd.to_string());
        sf.push(quote! { #fd: Vec::new(), });
        sc.push(quote! { self.#fd.push(row.#fd); });
        bc.push(quote! { ArcArr::from_vec(self.#fd), });
    }

    sf.push(quote! { _e: ::std::marker::PhantomData });

    (nm, sf, sc, bc)
}

// Generate the building result in `fn build`
fn gen_bd_res(
    e_type: &str,
    schema_len: usize,
    build_ctt: Vec<TokenStream>,
    names: Vec<String>,
) -> TokenStream {
    match e_type {
        CHUNK => quote! {
            Ok(ChunkArr::try_new(vec![ #(#build_ctt)* ])?)
        },
        ARRAA => quote! {
            Ok([ #(#build_ctt)* ])
        },
        BATCH => quote! {
            FxBatch::try_new_with_names(vec![ #(#build_ctt)* ], [ #(#names),* ])
        },
        TABLE => quote! {
            Ok(FxTable::<#schema_len, ArcArr>::new_with_names([ #(#build_ctt)* ], [ #(#names),* ]))
        },
        _ => panic!("Unsupported type"),
    }
}

/// Generate impl eclectic builder generator
fn gen_impl_ebg(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    build_name: &Ident,
) -> TokenStream {
    match e_type {
        CHUNK => quote! {
            impl FxChunkBuilderGenerator for #struct_name {
                type ChunkBuilder = #build_name<ChunkArr>;
            }
        },
        ARRAA => quote! {
            impl FxArraaBuilderGenerator<#schema_len> for #struct_name {
                type ArraaBuilder = #build_name<[ArcArr; #schema_len]>;
            }
        },
        BATCH => quote! {
            impl FxBatchBuilderGenerator for #struct_name {
                type BatchBuilder = #build_name<FxBatch>;
            }
        },
        TABLE => quote! {
            impl FxTableBuilderGenerator<#schema_len> for #struct_name {
                type TableBuilder = #build_name<FxTable<#schema_len, ArcArr>>;
            }
        },
        _ => panic!("Unsupported type"),
    }
}

/// impl eclectic
///
/// - ChunkArr
/// - FxBatch
/// - FxTable
fn gen_impl_eclectic(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let (names, self_ctt, stack_ctt, build_ctt) = gen_cct(named_fields);

    let eclectic_type = gen_eclectic_type(schema_len, e_type);

    let build_res = gen_bd_res(e_type, schema_len, build_ctt, names);

    let impl_build_gen = gen_impl_ebg(e_type, schema_len, struct_name, build_name);

    quote! {
        impl FxEclecticBuilder<#struct_name, #eclectic_type> for #build_name<#eclectic_type> {
            fn new() -> Self {
                Self {
                    #(#self_ctt)*
                }
            }

            fn stack(&mut self, row: #struct_name)-> &mut Self {
                #(#stack_ctt)*

                self
            }

            fn build(self) -> FxResult<#eclectic_type> {
                #build_res
            }
        }

        #impl_build_gen
    }
}

fn gen_multiple_impl_eclectic(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let impls = match e_type {
        CHUNK => {
            vec![
                // no schema
                gen_impl_eclectic(CHUNK, schema_len, struct_name, build_name, named_fields),
            ]
        }
        BATCH => {
            vec![
                // no schema (for collection generator)
                gen_impl_eclectic(CHUNK, schema_len, struct_name, build_name, named_fields),
                // schema
                gen_impl_eclectic(BATCH, schema_len, struct_name, build_name, named_fields),
            ]
        }
        TABLE => {
            vec![
                // no schema (for collection generator)
                gen_impl_eclectic(ARRAA, schema_len, struct_name, build_name, named_fields),
                // schema
                gen_impl_eclectic(TABLE, schema_len, struct_name, build_name, named_fields),
            ]
        }
        _ => panic!("Unsupported type"),
    };

    quote! {
        #(#impls)*
    }
}

// ================================================================================================
// Container builder
// ================================================================================================

fn gen_collection_builder_struct(
    e_type: &str,
    schema_len: usize,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
) -> TokenStream {
    match e_type {
        CHUNK => quote! {
            struct #container_build_name {
                result: Vec<ChunkArr>,
                buffer: Option<#eclectic_build_name<ChunkArr>>
            }
        },
        BATCH => quote! {
            struct #container_build_name {
                result: FxBatches::<ChunkArr>,
                buffer: Option<#eclectic_build_name<ChunkArr>>
            }
        },
        TABLE => quote! {
            struct #container_build_name {
                result: FxTables::<#schema_len, ArcArr>,
                buffer: Option<#eclectic_build_name<[ArcArr; #schema_len]>>
            }
        },
        _ => panic!("Unsupported type"),
    }
}

/// collection types
///
/// return type:
/// 1. has_schema;
/// 2. Eclectic;
/// 3. EclecticCollection;
/// 4. result & buffer in `fn new()`.
fn gen_collection_type(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    named_fields: &NamedFields,
) -> (bool, TokenStream, TokenStream, TokenStream) {
    let fields_ctt = named_fields.iter().map(gen_arrow_field).collect::<Vec<_>>();

    match e_type {
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
        TABLE => (
            true,
            quote! { [ArcArr; #schema_len] },
            quote! { FxTables::<#schema_len, ArcArr> },
            quote! {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                let result = FxTables::<#schema_len, ArcArr>::empty_with_schema(schema);
                let buffer = Some(#struct_name::gen_arraa_builder());
            },
        ),
        _ => panic!("Unsupported type"),
    }
}

/// Generate impl collection builder generator
fn gen_impl_cbg(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    collection_build_name: &Ident,
) -> TokenStream {
    match e_type {
        CHUNK => quote! {
            impl FxChunksBuilderGenerator for #struct_name {
                type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                type ChunksBuilder = #collection_build_name;
            }
        },
        BATCH => quote! {
            impl FxChunkBatchesBuilderGenerator for #struct_name {
                type ChunkBuilder = #eclectic_build_name<ChunkArr>;

                type BatchesBuilder = #collection_build_name;
            }
        },
        TABLE => quote! {
            impl FxTablesBuilderGenerator<#schema_len> for #struct_name {
                type ArraaBuilder = #eclectic_build_name<[ArcArr; #schema_len]>;

                type TablesBuilder = #collection_build_name;
            }
        },
        _ => panic!("Unsupported type"),
    }
}

/// impl container
///
/// VecChunk
/// FxBatches
/// FxTables
fn gen_impl_container(
    e_type: &str,
    schema_len: usize,
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let (has_schema, eclectic_type, eclectic_collection, result_and_buffer) =
        gen_collection_type(e_type, schema_len, struct_name, named_fields);

    let impl_build_gen = gen_impl_cbg(
        e_type,
        schema_len,
        struct_name,
        eclectic_build_name,
        container_build_name,
    );

    quote! {
        impl FxCollectionBuilder<
            #has_schema, #eclectic_build_name<#eclectic_type>, #struct_name, #eclectic_collection, usize, #eclectic_type
        > for #container_build_name
        {
            fn new() -> FxResult<Self> {
                #result_and_buffer

                Ok(Self { result, buffer })
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

// ================================================================================================
// Main impl
// ================================================================================================

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let struct_name = input.ident.clone();
    let named_fields = named_fields(input);
    let schema_len = schema_len(&named_fields);

    // get the first attribute from "fx", default to BATCH
    let e_type = get_first_attribute(input, "fx").unwrap_or(BATCH.to_owned());

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
    let impl_container_row_build = gen_impl_container(
        &e_type,
        schema_len,
        &struct_name,
        &eclectic_build_name,
        &container_build_name,
        &named_fields,
    );

    let expanded = quote! {
        //
        #impl_from_sql_row

        //
        #eclectic_builder_struct

        //
        #impl_eclectic_row_build

        //
        #container_builder_struct

        //
        #impl_container_row_build
    };

    expanded
}
