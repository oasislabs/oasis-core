extern crate ekiden_tools;
extern crate protoc_grpcio;

fn main() {
    // Generate module file.
    // Must be done first to create src/generated directory
    ekiden_tools::generate_mod_with_imports(
        "src/generated",
        &["common", "roothash"],
        &["entity", "entity_grpc", "runtime", "runtime_grpc"],
    );

    // Root set to the core ekiden root so that common/api is in scope.
    protoc_grpcio::compile_grpc_protos(
        &["entity.proto", "runtime.proto"],
        &["src", "../../"],
        "src/generated",
    ).expect("failed to compile gRPC definitions");

    println!(
        "cargo:rerun-if-changed={}",
        "../../common/api/src/common.proto"
    );
    println!(
        "cargo:rerun-if-changed={}",
        "../../roothash/api/src/roothash.proto"
    );
    println!("cargo:rerun-if-changed={}", "src/runtime.proto");
    println!("cargo:rerun-if-changed={}", "src/entity.proto");
}
