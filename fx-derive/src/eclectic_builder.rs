//! file: eclectic_builder.rs
//! author: Jacob Xie
//! date: 2023/03/15 20:33:11 Wednesday
//! brief:

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

use crate::constant::*;
use crate::helper::*;

// ================================================================================================
// Eclectic builder
// ================================================================================================

/// Generate a builder struct for eclectic type. Note, a generic T is given as a phantom type, and
/// this is the simpliest way to destinguish which builder to be used during the genarator's impl,
/// details in [tests/fx_builder_test.rs] the third part.
pub(crate) fn gen_eclectic_builder_struct(
    build_name: &Ident,
    named_fields: &NamedFields,
) -> TokenStream {
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
        BUNDLE => quote! {
            Ok(FxBundle::<#schema_len, ArcArr>::new_with_names([ #(#build_ctt)* ], [ #(#names),* ]))
        },
        TABLE => quote! {
            FxTable::<#schema_len>::try_new_with_names(vec![ #(#build_ctt)* ], [ #(#names),* ])
        },
        TABULAR => quote! {
            FxTabular::try_new_with_names(vec![ #(#build_ctt)* ], [ #(#names),* ])
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
        BUNDLE => quote! {
            impl FxBundleBuilderGenerator<#schema_len> for #struct_name {
                type BundleBuilder = #build_name<FxBundle<#schema_len, ArcArr>>;
            }
        },
        _ => panic!("Unsupported type"),
    }
}

/// impl eclectic
///
/// - ChunkArr
/// - FxBatch
/// - FxBundle
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

pub(crate) fn gen_multiple_impl_eclectic(
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
        BUNDLE => {
            vec![
                // no schema (for collection generator)
                gen_impl_eclectic(ARRAA, schema_len, struct_name, build_name, named_fields),
                // schema
                gen_impl_eclectic(BUNDLE, schema_len, struct_name, build_name, named_fields),
            ]
        }
        TABLE => {
            vec![
                gen_impl_eclectic(ARRAA, schema_len, struct_name, build_name, named_fields),
                gen_impl_eclectic(CHUNK, schema_len, struct_name, build_name, named_fields),
                gen_impl_eclectic(BATCH, schema_len, struct_name, build_name, named_fields),
            ]
        }
        TABULAR => {
            vec![
                gen_impl_eclectic(ARRAA, schema_len, struct_name, build_name, named_fields),
                gen_impl_eclectic(CHUNK, schema_len, struct_name, build_name, named_fields),
                gen_impl_eclectic(BATCH, schema_len, struct_name, build_name, named_fields),
            ]
        }
        _ => panic!("Unsupported type"),
    };

    quote! {
        #(#impls)*
    }
}
