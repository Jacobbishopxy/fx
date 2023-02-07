use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Field, Ident, Type};

pub(crate) type NamedFields = Punctuated<Field, Comma>;

#[allow(dead_code)]
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
