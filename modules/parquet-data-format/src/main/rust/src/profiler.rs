use std::time::Instant;
use std::env;

pub fn profile_sorted_merge() -> Result<(), Box<dyn std::error::Error>> {
    let home_dir = env::var("HOME").unwrap_or_else(|_| "/Users/shaikumm/Downloads".to_string());

    let input_files = vec![
        format!("{}/Downloads/data_file_1.parquet", home_dir),
        format!("{}/Downloads/data_file_2.parquet", home_dir),
        format!("{}/Downloads/data_file_3.parquet", home_dir),
        format!("{}/Downloads/data_file_4.parquet", home_dir),
        format!("{}/Downloads/data_file_5.parquet", home_dir),
    ];

    let output_file = format!("{}/merged_sorted_output.parquet", home_dir);

    println!("=== PROFILING process_files_sorted ===");
    println!("Files: {:?}", input_files);

    let start = Instant::now();

    crate::parquet_merge_hybrid::merge_with_priority_queue(
        &input_files,
        &output_file,
        "timestamp",
        false, // ascending order
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
