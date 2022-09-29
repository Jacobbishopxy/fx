//! Derive

use proc_macro2::TokenStream;
use quote::quote;
use syn::{punctuated::Punctuated, token::Comma, Data, DeriveInput, Field, Fields, Type};

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

fn generated_bucket_infuse(named_fields: &NamedFields) -> TokenStream {
    // TODO

    todo!()
}

fn generated_builder_stack(named_fields: &NamedFields) -> TokenStream {
    // TODO

    todo!()
}

fn generated_schema(named_fields: &NamedFields) -> Vec<TokenStream> {
    named_fields
        .iter()
        .map(|f| match &f.ty {
            Type::Path(tp) => {
                let p = &tp.path;
                if p.is_ident("u8") {
                    quote!(crate::FxValueType::U8)
                } else if p.is_ident("u16") {
                    quote!(crate::FxValueType::U16)
                } else if p.is_ident("u32") {
                    quote!(crate::FxValueType::U32)
                } else if p.is_ident("u64") {
                    quote!(crate::FxValueType::U64)
                } else if p.is_ident("i8") {
                    quote!(crate::FxValueType::I8)
                } else if p.is_ident("i16") {
                    quote!(crate::FxValueType::I16)
                } else if p.is_ident("i32") {
                    quote!(crate::FxValueType::I32)
                } else if p.is_ident("i64") {
                    quote!(crate::FxValueType::I64)
                } else if p.is_ident("f32") {
                    quote!(crate::FxValueType::F32)
                } else if p.is_ident("f64") {
                    quote!(crate::FxValueType::F64)
                } else if p.is_ident("bool") {
                    quote!(crate::FxValueType::Bool)
                } else if p.is_ident("String") {
                    quote!(crate::FxValueType::String)
                } else {
                    quote!(crate::FxValueType::Null)
                }
            }
            _ => panic!("Only accept `TypePath`"),
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
    // let bucket_infuse = generated_bucket_infuse(&named_fields);
    // let builder_stack = generated_builder_stack(&named_fields);

    let expanded = quote! {
        impl FxDatagridTypedRowBuild<#schema_len> for #name {
            fn build(builder: DatagridRowWiseBuilder<#schema_len>) -> crate::FxResult<crate::Datagrid> {
                let mut bucket = #bucket;

                // for mut row in builder.into_iter() {
                //     #bucket_infuse
                // }

                // let mut vb = crate::DatagridColWiseBuilder::<#schema_len>::new();

                // #builder_stack

                todo!()
            }

            fn schema() -> crate::FxResult<crate::FxSchema<#schema_len>> {
                crate::FxSchema::<#schema_len>::try_from(vec![#(#schema),*])
            }
        }
    };

    expanded
}
