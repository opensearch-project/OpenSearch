/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Proc macro for FFM bridge functions.
//!
//! Wraps an `extern "C"` function body with `catch_unwind`. The body must
//! return `Result<i64, String>`. On success the `i64` is returned directly.
//! On `Err` or panic, the error message is heap-allocated and returned as a
//! negative pointer (negated `Box::into_raw` address).
//!
//! Java checks: if result < 0, call `native_error_message(-result)` to get
//! the message, then `native_error_free(-result)` to free it.
//!
//! # Usage
//!
//! ```ignore
//! #[ffm_safe]
//! #[no_mangle]
//! pub unsafe extern "C" fn my_func(arg: i64) -> i64 {
//!     do_work(arg).map_err(|e| e.to_string())
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat, ReturnType};

/// Wraps an `extern "C"` FFM function body in `std::thread::spawn(...).join()`
/// so that all Rust code (including allocations) runs on a Rust-owned thread
/// with properly initialized TLS. This prevents SIGSEGV in mimalloc when the
/// native library is loaded via dlopen into a JVM process.
///
/// Raw pointer parameters and return types are transmitted as `usize` to
/// satisfy `Send`. For void functions, panics in the spawned thread are
/// silently ignored. For functions returning a value, panics produce 0.
#[proc_macro_attribute]
pub fn ffm_thread(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let (spawn_args, shadow_lets) = build_ptr_casts(&sig.inputs);

    let ret_is_ptr = match &sig.output {
        ReturnType::Default => false,
        ReturnType::Type(_, ty) => quote!(#ty).to_string().contains('*'),
    };
    let has_return = !matches!(&sig.output, ReturnType::Default);
    let ret_type = match &sig.output {
        ReturnType::Type(_, ty) => Some(ty.as_ref()),
        _ => None,
    };

    let expanded = if !has_return {
        quote! {
            #(#attrs)*
            #vis #sig {
                #(#spawn_args)*
                let _ = ::std::thread::spawn(move || {
                    #(#shadow_lets)*
                    #body
                }).join();
            }
        }
    } else if ret_is_ptr {
        let rty = ret_type.unwrap();
        quote! {
            #(#attrs)*
            #vis #sig {
                #(#spawn_args)*
                let __ret = ::std::thread::spawn(move || -> usize {
                    #(#shadow_lets)*
                    let __val: #rty = #body;
                    __val as usize
                }).join().unwrap_or(0);
                __ret as #rty
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #vis #sig {
                #(#spawn_args)*
                ::std::thread::spawn(move || {
                    #(#shadow_lets)*
                    #body
                }).join().unwrap_or(0 as _)
            }
        }
    };

    expanded.into()
}

fn build_ptr_casts(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
) -> (Vec<proc_macro2::TokenStream>, Vec<proc_macro2::TokenStream>) {
    let mut spawn_args = Vec::new();
    let mut shadow_lets = Vec::new();

    for arg in inputs {
        if let FnArg::Typed(pat_type) = arg {
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                let name = &pat_ident.ident;
                let ty = &pat_type.ty;
                let ty_str = quote!(#ty).to_string();
                if ty_str.contains('*') {
                    let usize_name = syn::Ident::new(
                        &format!("__{}_usize", name),
                        name.span(),
                    );
                    spawn_args.push(quote! { let #usize_name = #name as usize; });
                    shadow_lets.push(quote! { let #name = #usize_name as #ty; });
                }
            }
        }
    }

    (spawn_args, shadow_lets)
}

#[proc_macro_attribute]
pub fn ffm_safe(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let (spawn_args, shadow_lets) = build_ptr_casts(&sig.inputs);

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #(#spawn_args)*
            ::std::thread::spawn(move || -> i64 {
                #(#shadow_lets)*
                match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(
                    || -> ::std::result::Result<i64, ::std::string::String> #body
                )) {
                    Ok(Ok(v)) => v,
                    Ok(Err(msg)) => native_bridge_common::error::into_error_ptr(msg),
                    Err(panic) => {
                        let msg = if let Some(s) = panic.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = panic.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown panic".to_string()
                        };
                        native_bridge_common::error::into_error_ptr(msg)
                    }
                }
            }).join().unwrap_or_else(|_| native_bridge_common::error::into_error_ptr("thread join failed".to_string()))
        }
    };

    expanded.into()
}
