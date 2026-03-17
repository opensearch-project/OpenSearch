/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::time::Instant;
use std::env;

pub fn profile_sorted_merge() -> Result<(), Box<dyn std::error::Error>> {
    let home_dir = env::var("HOME").unwrap_or_else(|_| "/Users/shaikumm/Downloads".to_string());

    let input_files = vec![
        format!("{}/Downloads/_parquet_file_generation_25.parquet", home_dir),
        format!("{}/Downloads/_parquet_file_generation_26.parquet", home_dir),
        format!("{}/Downloads/_parquet_file_generation_27.parquet", home_dir),
        format!("{}/Downloads/_parquet_file_generation_28.parquet", home_dir),
        format!("{}/Downloads/_parquet_file_generation_29.parquet", home_dir),
    ];

    let output_file = format!("{}/merged_sorted_output.parquet", home_dir);

    println!("=== PROFILING process_files_sorted ===");
    println!("Files: {:?}", input_files);

    let start = Instant::now();

    crate::parquet_merge_stream::merge_streaming(
        &input_files,
        &output_file,
        "TempIndex",
        "EventDate"
    )?;

    let duration = start.elapsed();
    println!("Execution time: {:?}", duration);
    println!("Output: {}", output_file);

    Ok(())
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn test_profile_sorted_merge() {
        if let Err(e) = profile_sorted_merge() {
            println!("Profile test failed: {}", e);
        }
    }
}
