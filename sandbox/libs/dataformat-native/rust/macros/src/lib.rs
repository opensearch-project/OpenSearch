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
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn ffm_safe(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let fn_name = input.sig.ident.to_string();
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            native_bridge_common::error::ffm_wrap(
                #fn_name,
                ::std::panic::AssertUnwindSafe(
                    || -> ::std::result::Result<i64, ::std::string::String> #body
                ),
            )
        }
    };

    expanded.into()
}
