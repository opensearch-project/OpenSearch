fn main() {
    let proto_dir = "../../../libs/vectorized-exec-spi/src/main/proto";
    prost_build::compile_protos(
        &[
            format!("{}/metrics/tokio_metrics.proto", proto_dir),
            format!("{}/metrics/datafusion_stats.proto", proto_dir),
        ],
        &[proto_dir],
    )
    .unwrap();
}
