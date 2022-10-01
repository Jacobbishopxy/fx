//! Derive

use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Data, DeriveInput, Field, Fields, Ident, Type};

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

#[allow(dead_code)]
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
    match ty {
        Type::Path(tp) => {
            let path = &tp.path;
            let is_option = tp.qself.is_none()
                && path.leading_colon.is_none()
                && path.segments.len() == 1
                && path.segments.iter().next().unwrap().ident == "Option";

            if is_option {
                match &path.segments.first().unwrap().arguments {
                    syn::PathArguments::AngleBracketed(ab) => {
                        let ga = ab.args.first().unwrap();
                        match ga {
                            syn::GenericArgument::Type(Type::Path(t)) => {
                                (true, t.path.segments.first().unwrap().ident.clone())
                            }
                            _ => panic!("type mismatch"),
                        }
                    }
                    _ => panic!("type mismatch"),
                }
            } else {
                (false, path.segments.first().unwrap().ident.clone())
            }
        }
        _ => panic!("type mismatch"),
    }
}

fn generated_bucket(named_fields: &NamedFields) -> TokenStream {
    let fields = named_fields
        .iter()
        .map(|f| {
            let ty = &f.ty;
            quote! { Vec::<#ty>::new() }
        })
        .collect::<Vec<_>>();

    quote! {
        (#(#fields),*)
    }
}

fn path_gen_bucket_infuse(ty: &Ident, i: usize, is_option: bool) -> TokenStream {
    let idx = syn::Index::from(i);
    match &ty.to_string()[..] {
        "u8" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_u8().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_u8().unwrap());
                }
            }
        }
        "u16" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_u16().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_u16().unwrap());
                }
            }
        }
        "u32" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_u32().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_u32().unwrap());
                }
            }
        }
        "u64" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_u64().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_u64().unwrap());
                }
            }
        }
        "i8" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_i8().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_i8().unwrap());
                }
            }
        }
        "i16" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_i16().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_i16().unwrap());
                }
            }
        }
        "i32" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_i32().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_i32().unwrap());
                }
            }
        }
        "i64" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_i64().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_i64().unwrap());
                }
            }
        }
        "f32" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_f32().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_f32().unwrap());
                }
            }
        }
        "f64" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_f64().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_f64().unwrap());
                }
            }
        }
        "bool" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_bool().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_bool().unwrap());
                }
            }
        }
        "String" => {
            if is_option {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_opt_string().unwrap());
                }
            } else {
                quote! {
                    bucket.#idx.push(row.take_uncheck(#idx).take_string().unwrap());
                }
            }
        }
        _ => panic!("unsupported type"),
    }
}

fn generated_bucket_infuse(named_fields: &NamedFields) -> Vec<TokenStream> {
    named_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let (is_option, ty) = get_option_type(&f.ty);
            path_gen_bucket_infuse(&ty, i, is_option)
        })
        .collect::<Vec<_>>()
}

fn generated_builder_stack(named_fields: &NamedFields) -> Vec<TokenStream> {
    (0..named_fields.len())
        .map(|i| {
            let idx = syn::Index::from(i);
            quote! {
                builder.stack(bucket.#idx);
            }
        })
        .collect::<Vec<_>>()
}

fn path_gen_schema_type(ty: &Ident, is_option: bool) -> TokenStream {
    match &ty.to_string()[..] {
        "u8" => {
            if is_option {
                quote!(crate::FxValueType::OptU8)
            } else {
                quote!(crate::FxValueType::U8)
            }
        }
        "u16" => {
            if is_option {
                quote!(crate::FxValueType::OptU16)
            } else {
                quote!(crate::FxValueType::U16)
            }
        }
        "u32" => {
            if is_option {
                quote!(crate::FxValueType::OptU32)
            } else {
                quote!(crate::FxValueType::U32)
            }
        }
        "u64" => {
            if is_option {
                quote!(crate::FxValueType::OptU64)
            } else {
                quote!(crate::FxValueType::U64)
            }
        }
        "i8" => {
            if is_option {
                quote!(crate::FxValueType::OptI8)
            } else {
                quote!(crate::FxValueType::I8)
            }
        }
        "i16" => {
            if is_option {
                quote!(crate::FxValueType::OptI16)
            } else {
                quote!(crate::FxValueType::I16)
            }
        }
        "i32" => {
            if is_option {
                quote!(crate::FxValueType::OptI32)
            } else {
                quote!(crate::FxValueType::I32)
            }
        }
        "i64" => {
            if is_option {
                quote!(crate::FxValueType::OptI64)
            } else {
                quote!(crate::FxValueType::I64)
            }
        }
        "f32" => {
            if is_option {
                quote!(crate::FxValueType::OptF32)
            } else {
                quote!(crate::FxValueType::F32)
            }
        }
        "f64" => {
            if is_option {
                quote!(crate::FxValueType::OptF64)
            } else {
                quote!(crate::FxValueType::F64)
            }
        }
        "bool" => {
            if is_option {
                quote!(crate::FxValueType::OptBool)
            } else {
                quote!(crate::FxValueType::Bool)
            }
        }
        "String" => {
            if is_option {
                quote!(crate::FxValueType::OptString)
            } else {
                quote!(crate::FxValueType::String)
            }
        }
        _ => panic!("unsupported type"),
    }
}

fn generated_schema(named_fields: &NamedFields) -> Vec<TokenStream> {
    named_fields
        .iter()
        .map(|f| {
            let (is_option, ty) = get_option_type(&f.ty);
            path_gen_schema_type(&ty, is_option)
        })
        .collect::<Vec<_>>()
}

pub(crate) fn impl_fx(input: &DeriveInput) -> TokenStream {
    // name of the struct
    let name = input.ident.clone();
    let named_fields = named_fields(input);

    let schema_len = schema_len(&named_fields);
    let schema = generated_schema(&named_fields);
    let bucket = generated_bucket(&named_fields);
    let bucket_infuse = generated_bucket_infuse(&named_fields);
    let builder_stack = generated_builder_stack(&named_fields);

    let expanded = quote! {
        impl FxDatagridTypedRowBuild<#schema_len> for #name {
            fn build(builder: DatagridRowWiseBuilder<#schema_len>) -> crate::FxResult<crate::Datagrid> {
                let mut bucket = #bucket;

                for mut row in builder.into_iter() {
                    #(#bucket_infuse)*
                }

                let mut builder = crate::DatagridColWiseBuilder::<#schema_len>::new();

                #(#builder_stack)*

                builder.build()
            }

            fn schema() -> crate::FxResult<crate::FxSchema<#schema_len>> {
                crate::FxSchema::<#schema_len>::try_from(vec![#(#schema),*])
            }
        }
    };

    expanded
}
