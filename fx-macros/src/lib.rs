//! Fx macros

use syn::{parse_macro_input, DeriveInput};

// mod deprecated;
mod dr;

use dr::*;

#[proc_macro_derive(FX, attributes(fx))]
pub fn derive_fx(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    let stream = impl_fx(&input);

    // Debug use:
    // println!("{}", &stream);

    proc_macro::TokenStream::from(stream)
}
