//! file: helper.rs
//! author: Jacob Xie
//! date: 2023/02/23 20:16:27 Thursday
//! brief: Helper functions

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Data, DeriveInput, Field, Fields, Ident, Type};

use crate::constant::*;

pub(crate) type NamedFields = Punctuated<Field, Comma>;

pub(crate) fn schema_len(named_fields: &NamedFields) -> usize {
    named_fields.len()
}

fn path_is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(tp) => {
            let path = &tp.path;
            tp.qself.is_none()
                && path.leading_colon.is_none()
                && path.segments.len() == 1
                && path.segments.iter().next().unwrap().ident == "Option"
        }
        _ => panic!("type mismatch"),
    }
}

fn get_option_type(ty: &Type) -> (bool, Ident) {
    let is_option = path_is_option(ty);

    if let Type::Path(tp) = ty {
        let path = &tp.path;
        if is_option {
            match &path.segments.first().unwrap().arguments {
                syn::PathArguments::AngleBracketed(ab) => {
                    let ga = ab.args.first().unwrap();
                    match ga {
                        syn::GenericArgument::Type(Type::Path(t)) => {
                            return (true, t.path.segments.first().unwrap().ident.clone());
                        }
                        _ => panic!("type mismatch"),
                    }
                }
                _ => panic!("type mismatch"),
            }
        } else {
            return (false, path.segments.first().unwrap().ident.clone());
        }
    }

    panic!("type mismatch")
}

pub(crate) fn get_option_type_name(ty: &Type) -> (bool, String) {
    let (is_option, ident) = get_option_type(ty);
    (is_option, ident.to_string())
}

/// extract attributes from a specified `attr_mark`.
/// For instance, if chk = Some(FxBatches), then use ChunkArr as Eclectic param in row-builders;
pub(crate) fn get_attributes(input: &DeriveInput, attr_mark: &str) -> Option<Vec<String>> {
    input
        .attrs
        .iter()
        .find(|a| a.path.segments[0].ident == attr_mark)
        .map(|a| match a.parse_meta().unwrap() {
            syn::Meta::List(syn::MetaList { nested, .. }) => match nested.first().unwrap() {
                syn::NestedMeta::Meta(m) => m
                    .path()
                    .segments
                    .iter()
                    .map(|s| s.ident.to_string())
                    .collect::<Vec<_>>(),
                _ => panic!("Unsupported nested"),
            },
            _ => panic!("Unsupported attribute form"),
        })
}

pub(crate) fn get_first_attribute(input: &DeriveInput, attr_mark: &str) -> Option<String> {
    get_attributes(input, attr_mark).and_then(|v| v.first().cloned())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

/// filter useless attribute
pub(crate) fn filter_attributes(opt_attr: String) -> Option<String> {
    if FX_OPTIONS.contains(&opt_attr.as_str()) {
        Some(opt_attr)
    } else {
        None
    }
}

/// turn ast into `Punctuated<Field, Comma>`, and filter out any type that is not a Rust struct
pub(crate) fn named_fields(ast: &DeriveInput) -> NamedFields {
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
pub(crate) fn gen_eclectic_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}EclecticBuild", struct_name)
}

/// generate container builder and its generator
pub(crate) fn gen_container_build_name(struct_name: &Ident) -> Ident {
    format_ident!("__{}EcLecticContainerBuild", struct_name)
}

/// generate eclectic type by string
pub(crate) fn gen_eclectic_type(schema_len: usize, s: &str) -> TokenStream {
    match s {
        CHUNK => quote! {ChunkArr},
        ARRAA => quote! {[ArcArr; #schema_len]},
        BATCH => quote! {FxBatch},
        BUNDLE => quote! {FxBundle::<#schema_len, ArcArr>},
        TABLE => quote! {ArcArr::<#schema_len>},
        _ => quote! {FxBatch}, // default to FxBatch
    }
}

/// generate container type by string
#[allow(dead_code)]
pub(crate) fn gen_container_type(schema_len: usize, s: &str) -> TokenStream {
    match s {
        CHUNK => quote! {Vec<ChunkArr>},
        BATCH => quote! {FxBatches::<ChunkArr>},
        BUNDLE => quote! {FxBundles::<#schema_len, ArcArr>},
        TABLE => quote! {FxTable::<#schema_len>},
        _ => quote! {FxBatches::<ChunkArr>}, // default to FxBatches
    }
}

/// generate arrow's field
pub(crate) fn gen_arrow_field(f: &Field) -> TokenStream {
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
