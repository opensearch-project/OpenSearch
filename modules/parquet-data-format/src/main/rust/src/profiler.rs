/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
//! # Parquet Merge Profiler
//! 
//! This profiler benchmarks the performance of merging multiple parquet files.
//! It looks for parquet files with a specific naming pattern and merges them
//! into a single sorted output file.
//! 
//! ## Usage
//! 
//! ### Command Line
//! ```bash
//! # Navigate to the rust directory
//! cd modules/parquet-data-format/src/main/rust
//! 
//! # Run with cargo (builds automatically)
//! cargo run --bin profiler -- /path/to/your/parquet/files
//! 
//! # With output directory
//! cargo run --bin profiler -- /path/to/input /path/to/output
//! 
//! # With custom file prefix
//! cargo run --bin profiler -- /path/to/input /path/to/output custom_prefix_
//! ```
//! 
//! ### Build and Run Binary
//! ```bash
//! # Build release binary
//! cargo build --release --bin profiler
//! 
//! # Run the built binary
//! ./target/release/profiler /path/to/your/parquet/files
//! ```
//! 
//! ### Expected File Structure
//! The profiler looks for files with this pattern:
//! ```
//! /your/input/directory/
//! ├── _parquet_file_generation_25.parquet
//! ├── _parquet_file_generation_26.parquet
//! ├── _parquet_file_generation_27.parquet
//! ├── _parquet_file_generation_28.parquet
//! └── _parquet_file_generation_29.parquet
//! ```
//! 
//! ### Parameters
//! - `input_dir`: Directory containing parquet files (REQUIRED)
//! - `output_dir`: Directory for output file (default: same as input_dir)
//! - `file_prefix`: Prefix for input files (default: '_parquet_file_generation_')
//! 
//! ### Output
//! - Merged file: `merged_sorted_output.parquet`
//! - Performance timing information
//! - File processing statistics
*/
use std::time::Instant;
use std::path::PathBuf;

pub fn profile_sorted_merge() -> Result<(), Box<dyn std::error::Error>> {
    profile_sorted_merge_with_config(None, None, None)
}

pub fn profile_sorted_merge_with_config(
    input_dir: Option<&str>,
    output_dir: Option<&str>,
    file_prefix: Option<&str>
) -> Result<(), Box<dyn std::error::Error>> {
    // Get base directory from user input - required
    let base_dir = match input_dir {
        Some(dir) => PathBuf::from(dir),
        None => {
            return Err("Input directory is required. Please provide input directory as first argument.".into());
        }
    };

    let output_base = match output_dir {
        Some(dir) => PathBuf::from(dir),
        None => base_dir.clone()
    };

    let prefix = file_prefix.unwrap_or("_parquet_file_generation_");

    // Generate input file paths
    let input_files: Vec<String> = (25..=29)
        .map(|i| base_dir.join(format!("{}{}.parquet", prefix, i)).to_string_lossy().to_string())
        .collect();

    let output_file = output_base.join("merged_sorted_output.parquet").to_string_lossy().to_string();

    // Validate that input directory exists
    if !base_dir.exists() {
        return Err(format!("Input directory does not exist: {}", base_dir.display()).into());
    }

    // Validate that at least one input file exists
    let existing_files: Vec<_> = input_files.iter()
        .filter(|file| std::path::Path::new(file).exists())
        .collect();
    
    if existing_files.is_empty() {
        return Err(format!(
            "No input files found in directory: {}. Expected files with prefix: {}", 
            base_dir.display(), 
            prefix
        ).into());
    }

    println!("=== PROFILING process_files_sorted ===");
    println!("Input directory: {}", base_dir.display());
    println!("Output directory: {}", output_base.display());
    println!("Files found: {}/{}", existing_files.len(), input_files.len());
    println!("Files: {:?}", existing_files);

    let start = Instant::now();

    // Use only existing files for the merge
    let existing_file_paths: Vec<String> = existing_files.into_iter().cloned().collect();
    
    crate::parquet_merge_stream::merge_streaming(
        &existing_file_paths,
        &output_file,
        "TempIndex",
        "EventDate"
    )?;

    let duration = start.elapsed();
    println!("Execution time: {:?}", duration);
    println!("Output: {}", output_file);

    Ok(())
}


