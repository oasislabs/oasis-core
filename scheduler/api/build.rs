extern crate ekiden_tools;
extern crate protoc_grpcio;

fn main() {
    // Generate module file.
    // Must be done first to create src/generated directory
    ekiden_tools::generate_mod_with_imports(
        "src/generated",
        &["runtime"],
        &["scheduler", "scheduler_grpc"],
    );

    // Root set to the core ekiden root so that common/api is in scope.
    protoc_grpcio::compile_grpc_protos(&["scheduler.proto"], &["src", "../../"], "src/generated")
        .expect("failed to compile gRPC definitions");

    println!(
        "cargo:rerun-if-changed={}",
        "../../registry/api/src/runtime.proto"
    );
    println!("cargo:rerun-if-changed={}", "src/scheduler.proto");
}
