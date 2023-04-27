fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("./src/executor")
        .compile(&["./proto/executor.proto"], &["proto"])?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("./src/scheduler")
        .compile(&["./proto/scheduler.proto"], &["proto"])?;

    std::fs::rename("src/scheduler/scheduler.rs", "src/scheduler/proto.rs")?;
    std::fs::rename("src/executor/executor.rs", "src/executor/proto.rs")?;

    Ok(())
}
