fn main() {
    if let Err(e) =
        parquet_dataformat_jni::profiler::profile_sorted_merge()
    {
        eprintln!("Profiler failed: {}", e);
    }
}
