/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    
    let (input_dir, output_dir, file_prefix) = match args.len() {
        1 => {
            eprintln!("Error: Input directory is required.");
            eprintln!("Usage: {} <input_dir> [output_dir] [file_prefix]", args[0]);
            std::process::exit(1);
        },
        2 => (Some(args[1].as_str()), None, None),
        3 => (Some(args[1].as_str()), Some(args[2].as_str()), None),
        4 => (Some(args[1].as_str()), Some(args[2].as_str()), Some(args[3].as_str())),
        _ => {
            eprintln!("Usage: {} <input_dir> [output_dir] [file_prefix]", args[0]);
            eprintln!("  input_dir:   Directory containing parquet files (REQUIRED)");
            eprintln!("  output_dir:  Directory for output file (default: same as input_dir)");
            eprintln!("  file_prefix: Prefix for input files (default: '_parquet_file_generation_')");
            std::process::exit(1);
        }
    };

    if let Err(e) = parquet_dataformat_jni::profiler::profile_sorted_merge_with_config(
        input_dir, output_dir, file_prefix
    ) {
        eprintln!("Profiler failed: {}", e);
        std::process::exit(1);
    }
}