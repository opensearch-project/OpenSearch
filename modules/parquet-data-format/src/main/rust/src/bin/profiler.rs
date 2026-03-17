/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

fn main() {
    if let Err(e) =
        parquet_dataformat_jni::profiler::profile_sorted_merge()
    {
        eprintln!("Profiler failed: {}", e);
    }
}