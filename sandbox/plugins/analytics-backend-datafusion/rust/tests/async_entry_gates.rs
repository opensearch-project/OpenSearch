/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Source-level CI gates for the async reduce path (spec Part E item 6).
//!
//! These guard the load-bearing invariants structurally so a future edit can't silently
//! reintroduce a thread-pinning `block_on` into an async FFM entry, or resurrect the removed
//! blocking sender API:
//!
//!  - `df_stream_next_async` / `df_sender_send_async` bodies contain NO `block_on` (they must
//!    spawn-and-return; a `block_on` would pin the calling carrier and defeat the whole change).
//!  - the synchronous `df_stream_next` entry still exists (the shard-scan / fetch paths depend on
//!    it; this PR is coordinator-only and must not remove the sync pull).
//!  - `send_blocking` appears nowhere (the blocking sender API was replaced by try_send/clone_tx).
//!
//! Uses `CARGO_MANIFEST_DIR` so the paths are stable regardless of the test runner's cwd.

use std::fs;
use std::path::PathBuf;

fn read_src(rel: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push(rel);
    fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {}", p.display(), e))
}

/// Extracts the body of `pub unsafe extern "C" fn <name>(...) { ... }` by brace matching.
fn extern_fn_body(src: &str, name: &str) -> String {
    let sig = format!("fn {}(", name);
    let start = src.find(&sig).unwrap_or_else(|| panic!("fn {} not found", name));
    let open = src[start..].find('{').expect("fn has no body") + start;
    let bytes = src.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return src[open..=i].to_string();
                }
            }
            _ => {}
        }
        i += 1;
    }
    panic!("unbalanced braces for fn {}", name);
}

#[test]
fn async_stream_next_has_no_block_on() {
    let ffm = read_src("src/ffm.rs");
    let body = extern_fn_body(&ffm, "df_stream_next_async");
    assert!(
        !body.contains("block_on"),
        "df_stream_next_async must spawn-and-return, never block_on (would pin the carrier):\n{}",
        body
    );
    assert!(body.contains("spawn"), "df_stream_next_async must spawn the pull onto the IO runtime");
}

#[test]
fn async_sender_send_has_no_block_on() {
    let ffm = read_src("src/ffm.rs");
    let body = extern_fn_body(&ffm, "df_sender_send_async");
    assert!(
        !body.contains("block_on"),
        "df_sender_send_async must not block_on:\n{}",
        body
    );
}

#[test]
fn sync_stream_next_entry_still_exists() {
    // Coordinator-only scope: the shard-scan / fetch data-node paths still use the synchronous
    // df_stream_next. Removing it would silently flip shard execution onto the async path.
    let ffm = read_src("src/ffm.rs");
    assert!(
        ffm.contains("pub unsafe extern \"C\" fn df_stream_next("),
        "synchronous df_stream_next entry must remain for the shard-scan / fetch paths"
    );
}

#[test]
fn send_blocking_is_gone_everywhere() {
    for f in ["src/partition_stream.rs", "src/api.rs", "src/ffm.rs", "src/local_executor.rs"] {
        let src = read_src(f);
        assert!(
            !src.contains("send_blocking"),
            "{} still references send_blocking; the blocking sender API was replaced by try_send/clone_tx",
            f
        );
    }
}
