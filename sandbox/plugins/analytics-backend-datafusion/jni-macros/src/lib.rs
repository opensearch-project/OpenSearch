use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Wraps a JNI function body with `catch_unwind` to prevent Rust panics
/// from unwinding into Java.
#[proc_macro_attribute]
pub fn jni_safe(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let attr_str = attr.to_string();

    let panic_handler = if attr_str.contains("default") {
        // Extract the expression after "default = "
        let expr_str = attr_str
            .split('=')
            .nth(1)
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "()".to_string());
        let default_val: proc_macro2::TokenStream = expr_str.parse()
            .expect("Failed to parse default value expression");

        quote! {
            match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(move || {
                #body
            })) {
                Ok(result) => result,
                Err(panic) => {
                    let msg = if let Some(s) = panic.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "unknown panic".to_string()
                    };
                    eprintln!("[JNI PANIC] {}", msg);
                    #default_val
                }
            }
        }
    } else {
        quote! {
            match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(move || {
                #body
            })) {
                Ok(result) => result,
                Err(panic) => {
                    let msg = if let Some(s) = panic.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "unknown panic".to_string()
                    };
                    eprintln!("[JNI PANIC] {}", msg);
                }
            }
        }
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #panic_handler
        }
    };

    expanded.into()
}
