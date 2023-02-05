use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Field, Ident, Type};

type NamedFields = Punctuated<Field, Comma>;

#[allow(dead_code)]
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

#[allow(dead_code)]
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
