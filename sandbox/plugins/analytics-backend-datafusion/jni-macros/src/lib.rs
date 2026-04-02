/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
// Proc macro that wraps a JNI extern function body with panic-catching.
// On panic, throws a Java RuntimeException and returns a default value.
//
// Usage:
//   #[jni_safe]                          — for functions returning ()
//   #[jni_safe(default = 0)]             — for functions returning jlong, jint, etc.
//   #[jni_safe(default = std::ptr::null_mut())]  — for functions returning pointers
//
// The first parameter of the function MUST be `env: JNIEnv` (or `mut env: JNIEnv`).

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Expr, parse_str};

#[proc_macro_attribute]
pub fn jni_safe(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    // Parse default value from attribute, e.g. #[jni_safe(default = 0)]
    let default_expr: Expr = if attr.is_empty() {
        parse_str("()").unwrap()
    } else {
        let attr_str = attr.to_string();
        let value = attr_str
            .strip_prefix("default")
            .and_then(|s| s.trim().strip_prefix('='))
            .map(|s| s.trim())
            .unwrap_or("()");
        parse_str(value).unwrap()
    };

    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let __env_ptr = &mut env as *mut jni::JNIEnv;
            match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| {
                let env = unsafe { &mut *__env_ptr };
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
                    let __env = unsafe { &mut *__env_ptr };
                    let _ = __env.throw_new(
                        "java/lang/RuntimeException",
                        format!("Native panic: {}", msg),
                    );
                    #default_expr
                }
            }
        }
    };

    expanded.into()
}
