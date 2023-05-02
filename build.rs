fn main() -> Result<(), Box<dyn std::error::Error>> {
    let common_path = ".common";
    let common_rust_path = "crate::common::common";
    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .out_dir("./src/common")
        .compile(&["proto/common.proto"], &["proto"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("./src/executor")
        .extern_path(common_path, common_rust_path)
        .compile(&["./proto/executor.proto","proto/common.proto"], &["proto", "src/common"])?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("./src/scheduler")
        .extern_path(common_path, common_rust_path)
        .compile(&["proto/scheduler.proto","proto/common.proto"], &["proto", "src/common"])?;

    std::fs::rename("src/scheduler/scheduler.rs", "src/scheduler/proto.rs")?;
    std::fs::rename("src/executor/executor.rs", "src/executor/proto.rs")?;
    // std::fs::rename("src/common/common.rs", "src/common/proto.rs")?;

    Ok(())
}
