const PROTOS: &[&str] = &["src/protos/fileformat.proto", "src/protos/osmformat.proto"];

fn main() {
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={}", proto);
    }
    protobuf_codegen::Codegen::new()
        .pure()
        .cargo_out_dir("protos-gen")
        .includes(&["src/protos"])
        .inputs(PROTOS)
        .run_from_script();
}
