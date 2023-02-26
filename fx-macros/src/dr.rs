//! file: dr.rs
//! author: Jacob Xie
//! date: 2023/02/10 21:10:37 Friday
//! brief: Derive

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, Ident};

use crate::helper::*;

// ================================================================================================
// Constants
// ================================================================================================

const CHUNK: &str = "chunk";
const BATCH: &str = "batch";
const TABLE: &str = "table";

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
        BATCH => quote! {FxBatch},
        TABLE => quote! {FxTable::<#schema_len, ArcArr>},
        _ => quote! {ChunkArr}, // default to ChunkArr
    }
}

/// generate container type by string
#[allow(dead_code)]
fn gen_container_type(schema_len: usize, s: &str) -> TokenStream {
    match s {
        CHUNK => quote! {Vec<ChunkArr>},
        BATCH => quote! {FxBatches::<FxBatch>},
        TABLE => quote! {FxTables::<#schema_len, ArcArr>},
        _ => quote! {FxBatches::<FxBatch>}, // default to FxBatches
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

fn gen_eclectic_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
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

fn gen_cct(named_fields: &NamedFields) -> (Vec<String>, Vec<TokenStream>, Vec<TokenStream>) {
    let mut nm = vec![];
    let mut sc = vec![];
    let mut bc = vec![];

    for f in named_fields.iter() {
        let fd = f.ident.as_ref().unwrap();

        nm.push(fd.to_string());
        sc.push(quote! { self.#fd.push(row.#fd) });
        bc.push(quote! { ArcArr::from_vec(self.#fd) });
    }

    (nm, sc, bc)
}

fn gen_bd_res(
    e_type: &str,
    schema_len: usize,
    build_ctt: Vec<TokenStream>,
    names: Vec<String>,
) -> TokenStream {
    match e_type {
        CHUNK => quote! {
            Ok(ChunkArr::try_new(vec![ #(#build_ctt),* ])?)
        },
        BATCH => quote! {
            FxBatch::try_new_with_names(vec![ #(#build_ctt),* ], [ #(#names),* ])
        },
        TABLE => quote! {
            Ok(FxTable::<#schema_len, ArcArr>::new_with_names([ #(#build_ctt),* ], [ #(#names),* ]))
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
    let (names, stack_ctt, build_ctt) = gen_cct(named_fields);

    let eclectic_type = gen_eclectic_type(schema_len, e_type);

    let build_res = gen_bd_res(e_type, schema_len, build_ctt, names);

    // ================================================================================================
    // TODO
    // ================================================================================================

    quote! {
        impl FxEclecticBuilder<#struct_name, #eclectic_type> for #build_name {
            fn new() -> Self {
                Self::default()
            }

            fn stack(&mut self, row: #struct_name)-> &mut Self {
                #(#stack_ctt);*;

                self
            }

            fn build(self) -> FxResult<#eclectic_type> {
                #build_res
            }
        }

        impl FxEclecticBuilderGenerator<#eclectic_type> for #struct_name {
            type Builder = #build_name;

            fn gen_eclectic_row_builder() -> Self::Builder {
                #build_name::new()
            }
        }
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
            #[derive(Default)]
            struct #container_build_name {
                result: Vec<ChunkArr>,
                buffer: Option<#eclectic_build_name>
            }
        },
        BATCH => quote! {
            #[derive(Default)]
            struct #container_build_name {
                result: FxBatches::<FxBatch>,
                buffer: Option<#eclectic_build_name>
            }
        },
        TABLE => quote! {
            #[derive(Default)]
            struct #container_build_name {
            result: FxTables::<#schema_len, ArcArr>,
            buffer: Option<#eclectic_build_name>
        }
        },
        _ => panic!("Unsupported type"),
    }
}

/// collection types
///
/// return type:
/// 1. has_schema,
/// 2. Eclectic
/// 3. EclecticCollection
/// 4. result in `fn new()`
/// 5. push in `fn save()`
fn gen_collection_type(
    e_type: &str,
    schema_len: usize,
    named_fields: &NamedFields,
) -> (bool, TokenStream, TokenStream, TokenStream, TokenStream) {
    let fields_ctt = named_fields.iter().map(gen_arrow_field).collect::<Vec<_>>();

    match e_type {
        CHUNK => (
            false,
            quote! { ChunkArr },
            quote! { Vec<ChunkArr> },
            quote! {
                let result = Vec::<ChunkArr>::new();
            },
            quote! {
                self.result.push(caa);
            },
        ),
        BATCH => (
            true,
            quote! { FxBatch },
            quote! { FxBatches::<FxBatch> },
            quote! {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                let result = FxBatches::<FxBatch>::empty_with_schema(schema);
            },
            quote! {
                self.result.push(caa)?;
            },
        ),
        TABLE => (
            true,
            quote! { [ArcArr; #schema_len] },
            quote! { FxTables::<#schema_len, ArcArr> },
            quote! {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);
                let result = FxTables::<#schema_len, ArcArr>::empty_with_schema(schema);
            },
            quote! {
                self.result.push(caa)?;
            },
        ),
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
    let (has_schema, eclectic_type, eclectic_collection, res, psh) = gen_collection_type(
        //
        e_type,
        schema_len,
        named_fields,
    );

    quote! {
        impl FxCollectionBuilder<
            #has_schema, #eclectic_build_name, #struct_name, #eclectic_collection, usize, #eclectic_type
        > for #container_build_name
        {
            fn new() -> FxResult<Self> {
                #res

                let buffer = Some(<#struct_name as FxEclecticBuilderGenerator<#eclectic_type>>::gen_eclectic_row_builder());

                Ok(Self { result, buffer })
            }

            fn mut_buffer(&mut self) -> Option<&mut #eclectic_build_name> {
                self.buffer.as_mut()
            }

            fn set_buffer(&mut self, buffer: #eclectic_build_name) {
                self.buffer = Some(buffer);
            }

            fn take_buffer(&mut self) -> Option<#eclectic_build_name> {
                self.buffer.take()
            }

            fn mut_result(&mut self) -> &mut #eclectic_collection {
                &mut self.result
            }

            fn take_result(self) -> #eclectic_collection {
                self.result
            }
        }

        impl FxCollectionBuilderGenerator<
            #has_schema, #eclectic_build_name, #struct_name, #eclectic_collection, usize, #eclectic_type
        > for #struct_name {
            type Builder = #container_build_name;

            fn gen_eclectic_collection_row_builder() -> FxResult<Self::Builder> {
                Self::Builder::new()
            }
        }
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
    let impl_eclectic_row_build = gen_impl_eclectic(
        &e_type,
        schema_len,
        &struct_name,
        &eclectic_build_name,
        &named_fields,
    );

    // auto generated code (container)
    // let container_build_name = gen_container_build_name(&struct_name);
    // let container_builder_struct = gen_collection_builder_struct(
    //     &e_type,
    //     schema_len,
    //     &eclectic_build_name,
    //     &container_build_name,
    // );
    // let impl_container_row_build = gen_impl_container(
    //     &e_type,
    //     schema_len,
    //     &struct_name,
    //     &eclectic_build_name,
    //     &container_build_name,
    //     &named_fields,
    // );

    let expanded = quote! {
        //
        #impl_from_sql_row

        //
        #eclectic_builder_struct

        //
        #impl_eclectic_row_build

        //
        // #container_builder_struct

        //
        // #impl_container_row_build
    };

    expanded
}
