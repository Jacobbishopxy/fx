//! file: dr.rs
//! author: Jacob Xie
//! date: 2023/02/10 21:10:37 Friday
//! brief: Derive

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, Ident};

use crate::helper::{get_option_type_name, NamedFields};

// ================================================================================================
// Constants
// ================================================================================================

#[allow(dead_code)]
const FX_GRID: &str = "FxGrid";
#[allow(dead_code)]
const FX_BATCH: &str = "FxBatch";

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

/// generate chunking builder and its generator
fn gen_chunking_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}ChunkingBuild", struct_name)
}

/// generate container builder and its generator
fn gen_container_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}ContainerBuild", struct_name)
}

/// extract the first attribute from `fx`.
/// For instance, if chk = Some(FxBundle), then use FxGrid as Chunking param in row-builders;
/// otherwise, default to FxGrid
#[allow(dead_code)]
fn get_gx_attribute(input: &DeriveInput) -> Option<String> {
    // test case is in `grid.rs`
    input
        .attrs
        .iter()
        .find(|a| a.path.segments[0].ident == "fx")
        .map(|a| match a.parse_meta().unwrap() {
            syn::Meta::List(syn::MetaList { nested, .. }) => match nested.first().unwrap() {
                syn::NestedMeta::Meta(m) => m.path().segments.first().unwrap().ident.to_string(),
                _ => panic!("Unsupported nested"),
            },
            _ => panic!("Unsupported attribute form"),
        })
}

/// generate chunking type by string
#[allow(dead_code)]
fn gen_chunking_type(s: String) -> TokenStream {
    match s.as_str() {
        FX_GRID => quote! {crate::FxGrid},
        FX_BATCH => quote! {crate::FxBatch},
        _ => quote! {crate::FxGrid}, // default to FxGrid
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
// Chunking builder
// ================================================================================================

fn gen_chunking_builder_struct(build_name: &Ident, named_fields: &NamedFields) -> TokenStream {
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

fn gen_impl_chunking(
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
        impl crate::cont::FxChunkingRowBuilder<#struct_name,crate::FxGrid> for #build_name {
            fn new() -> Self {
                Self::default()
            }

            fn stack(&mut self, row: #struct_name)-> &mut Self {
                #(#stack_ctt);*;

                self
            }

            fn build(self) -> crate::error::FxResult<crate::FxGrid> {
                crate::FxGrid::try_from(vec![
                    #(#build_ctt),*
                ])
            }
        }

        impl crate::cont::FxChunkingRowBuilderGenerator<crate::FxGrid> for #struct_name {
            type Builder = #build_name;

            fn gen_chunking_row_builder() -> Self::Builder {
                #build_name::new()
            }
        }
    }
}

// ================================================================================================
// Container builder
// ================================================================================================

fn gen_container_builder_struct(
    chunking_build_name: &Ident,
    container_build_name: &Ident,
) -> TokenStream {
    quote! {
        #[derive(Default)]
        struct #container_build_name {
            result: crate::FxBundle<crate::FxGrid>,
            buffer: Option<#chunking_build_name>
        }
    }
}

fn gen_impl_container(
    struct_name: &Ident,
    chunking_build_name: &Ident,
    container_build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
    let fields_ctt = named_fields.iter().map(gen_arrow_field).collect::<Vec<_>>();

    quote! {
        impl crate::cont::FxContainerRowBuilder<#chunking_build_name, #struct_name, crate::FxBundle<crate::FxGrid>, usize, crate::FxGrid>
            for #container_build_name
        {
            fn new() -> crate::FxResult<Self>
            where
                Self: Sized,
            {
                let fields = vec![#(#fields_ctt),*];

                let result = crate::FxBundle::<crate::FxGrid>::new_empty_by_fields(fields)?;

                let buffer = Some(#struct_name::gen_chunking_row_builder());

                Ok(Self {result, buffer})
            }

            fn stack(&mut self, row: #struct_name) -> &mut Self {
                match self.buffer.as_mut() {
                    Some(b) => {
                        b.stack(row);
                    }
                    None => {
                        let mut buffer = #struct_name::gen_chunking_row_builder();
                        buffer.stack(row);
                        self.buffer = Some(buffer);
                    }
                };

                self
            }

            fn save(&mut self) -> crate::FxResult<&mut Self> {
                let grid = self.buffer.take().unwrap().build()?;
                self.result.push(grid)?;

                Ok(self)
            }

            fn build(self) -> crate::FxBundle<crate::FxGrid> {
                self.result
            }
        }

        impl crate::cont::FxContainerRowBuilderGenerator<#chunking_build_name, #struct_name, crate::FxBundle<crate::FxGrid>, usize, crate::FxGrid> for #struct_name {
            type Builder = #container_build_name;

            fn gen_container_row_builder() -> crate::FxResult<Self::Builder> {
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
    // TODO

    // auto generated code (chunking)
    let chunking_name = gen_chunking_build_name(&struct_name);
    let impl_from_sql_row = gen_impl_from_sql_row(&struct_name, &named_fields);
    let chunking_builder_struct = gen_chunking_builder_struct(&chunking_name, &named_fields);
    let impl_chunking_row_build = gen_impl_chunking(&struct_name, &chunking_name, &named_fields);

    // auto generated code (container)
    let container_name = gen_container_build_name(&struct_name);
    let container_builder_struct = gen_container_builder_struct(&chunking_name, &container_name);
    let impl_container_row_build =
        gen_impl_container(&struct_name, &chunking_name, &container_name, &named_fields);

    let expanded = quote! {

        #impl_from_sql_row

        #chunking_builder_struct

        #impl_chunking_row_build

        #container_builder_struct

        #impl_container_row_build
    };

    expanded
}
