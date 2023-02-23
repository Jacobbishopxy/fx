//! file: dr.rs
//! author: Jacob Xie
//! date: 2023/02/10 21:10:37 Friday
//! brief: Derive

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, Ident};

use crate::constant::*;
use crate::helper::*;

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
#[allow(dead_code)]
fn gen_container_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}EcLecticContainerBuild", struct_name)
}

/// generate seq type by string
#[allow(dead_code)]
fn gen_seq_type(s: &str) -> TokenStream {
    match s {
        FX_ARC_ARR => quote! {ArcArr},
        FX_ARC_VEC => quote! {ArcVec},
        _ => quote! {ArcArr}, // default to ArcArr
    }
}

/// generate eclectic type by string
fn gen_eclectic_type(s: &str) -> TokenStream {
    match s {
        FX_VEC_ARC_ARR => quote! {Vec<ArcArr>},
        FX_VEC_ARC_VEC => quote! {Vec<ArcVec>},
        FX_CHUNK_ARR => quote! {ChunkArr},
        FX_BATCH => quote! {FxBatch},
        FX_TABLE => quote! {FxTable::<ArcArr>},
        _ => quote! {ChunkArr}, // default to ChunkArr
    }
}

/// generate container type by string
#[allow(dead_code)]
fn gen_container_type(s: &str) -> TokenStream {
    match s {
        FX_VEC_CHUNK => quote! {Vec<ChunkArr>},
        FX_MAP_CHUNK => quote! {Map<String, ChunkArr>},
        FX_BUNDLE => quote! {FxBatches},
        _ => quote! {FxBatches}, // default to FxBatches
    }
}

/// generate arrow's field
#[allow(dead_code)]
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
// Types selector from Attributes
// ================================================================================================

fn get_builder_str(opt_attributes: Option<Vec<String>>) -> (String, String) {
    // default types
    let mut eclectic_type = String::from(FX_CHUNK_ARR);
    let mut container_type = String::from(FX_BUNDLE);

    if let Some(attrs) = opt_attributes {
        for attr in attrs.iter() {
            if let Some(s) = get_eclectic_type(attr) {
                eclectic_type = s;
            }
            if let Some(s) = get_container_type(attr) {
                container_type = s;
            }
        }
    }

    (eclectic_type, container_type)
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

fn gen_cct2(eclectic_str: &str, build_ctt: Vec<TokenStream>, names: Vec<String>) -> TokenStream {
    match eclectic_str {
        FX_VEC_ARC_ARR => quote! {
            Ok(Vec<ArcArr>::new(vec![ #(#build_ctt),* ]))
        },
        FX_VEC_ARC_VEC => quote! {
            Ok(Vec<ArcVec>::new(vec![ #(#build_ctt),* ]))
        },
        FX_BATCH => quote! {
            FxBatch::try_new_with_names(vec![ #(#build_ctt),* ], [ #(#names),* ])
        },
        FX_TABLE => quote! {
            Ok(FxTable::<ArcArr>::new_with_names(vec![ #(#build_ctt),* ], [ #(#names),* ]))
        },
        _ => quote! {
            Ok(ChunkArr::try_new(vec![ #(#build_ctt),* ])?)
        },
    }
}

/// impl eclectic
///
/// - VecArcArr
/// - VecArcVec
/// - ChunkArr
/// - FxBatch
/// - FxTable
fn gen_impl_eclectic(
    eclectic_str: &str,
    struct_name: &Ident,
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let (names, stack_ctt, build_ctt) = gen_cct(named_fields);

    let eclectic_type = gen_eclectic_type(eclectic_str);

    let build_res = gen_cct2(eclectic_str, build_ctt, names);

    quote! {
        impl FxEclecticRowBuilder<#struct_name, #eclectic_type> for #build_name {
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

        impl FxEclecticRowBuilderGenerator<#eclectic_type> for #struct_name {
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

#[allow(dead_code)]
fn gen_container_builder_struct(
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
) -> TokenStream {
    quote! {
        #[derive(Default)]
        struct #container_build_name {
            result: Vec<ChunkArr>,
            buffer: Option<#eclectic_build_name>
        }
    }
}

#[allow(dead_code)]
fn gen_bundle_container_builder_struct(
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
) -> TokenStream {
    quote! {
        #[derive(Default)]
        struct #container_build_name {
            result: FxBatches,
            buffer: Option<#eclectic_build_name>
        }
    }
}

/// impl container
///
/// VecChunk
/// MapChunk
/// FxBatches
#[allow(dead_code)]
fn gen_impl_container(
    struct_name: &Ident,
    eclectic_build_name: &Ident,
    container_build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let fields_ctt = named_fields.iter().map(gen_arrow_field).collect::<Vec<_>>();

    quote! {
        impl FxEclecticCollectionRowBuilder<
            true, #eclectic_build_name, #struct_name, FxBatches, usize, ChunkArr
        > for #container_build_name
        {
            fn new() -> FxResult<Self>
            where
                Self: Sized,
            {
                let schema = ::arrow2::datatypes::Schema::from(vec![#(#fields_ctt),*]);

                let result = FxBatches::empty_with_schema(schema);

                let buffer = Some(#struct_name::gen_eclectic_row_builder());

                Ok(Self { result, buffer })
            }

            fn stack(&mut self, row: #struct_name) -> &mut Self {
                match self.buffer.as_mut() {
                    Some(b) => {
                        b.stack(row);
                    }
                    None => {
                        let mut buffer = #struct_name::gen_eclectic_row_builder();
                        buffer.stack(row);
                        self.buffer = Some(buffer);
                    }
                };

                self
            }

            fn save(&mut self) -> FxResult<&mut Self> {
                let caa = self.buffer.take().unwrap().build()?;
                self.result.push(caa)?;

                Ok(self)
            }

            fn build(self) -> FxBatches {
                self.result
            }
        }

        impl FxEclecticCollectionRowBuilderGenerator<
            true, #eclectic_build_name, #struct_name, FxBatches, usize, ChunkArr
        > for #struct_name {
            type Builder = #container_build_name;

            fn gen_eclectic_collection_row_builder() -> FxResult<Self::Builder> {
                #container_build_name::new()
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

    // attributes
    let option_attr = get_attributes(input, "fx");

    // builders
    let (e, _c) = get_builder_str(option_attr);
    // println!("{:?}", &e);
    // println!("{:?}", &c);

    // auto generated code (eclectic)
    let eclectic_name = gen_eclectic_build_name(&struct_name);
    let impl_from_sql_row = gen_impl_from_sql_row(&struct_name, &named_fields);
    let eclectic_builder_struct = gen_eclectic_builder_struct(&eclectic_name, &named_fields);
    let impl_eclectic_row_build =
        gen_impl_eclectic(&e, &struct_name, &eclectic_name, &named_fields);

    // auto generated code (container)
    // TODO:
    // let container_name = gen_container_build_name(&struct_name);
    // let container_builder_struct =
    //     gen_bundle_container_builder_struct(&eclectic_name, &container_name);
    // let impl_container_row_build =
    //     gen_impl_container(&struct_name, &eclectic_name, &container_name, &named_fields);

    let expanded = quote! {

        #impl_from_sql_row

        #eclectic_builder_struct

        #impl_eclectic_row_build

        // #container_builder_struct

        // #impl_container_row_build
    };

    expanded
}
