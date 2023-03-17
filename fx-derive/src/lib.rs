//! file: lib.rs
//! author: Jacob Xie
//! date: 2023/02/10 23:08:04 Friday
//! brief: ProcMacro FX

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod constant;
mod dr;
mod eclectic_builder;
mod helper;
mod receptacle_builder;
mod sql_impl;

use dr::*;

#[proc_macro_derive(FX, attributes(fx))]
pub fn derive_fx(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    let stream = impl_fx(&input);

    // Debug use:
    // println!("{}", &stream);

    TokenStream::from(stream)
}
