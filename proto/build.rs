use std::{ffi::OsStr, path::PathBuf, process::Command};

const PROTO_BASE_DIR: &str = "src/protos";
const PROTOS: &[&str] = &["fileformat.proto", "osmformat.proto"];

const PROTOC_VERSION: &str = "34.0";

struct ProtoC {
    protoc_bin: PathBuf,
    includes: Vec<PathBuf>,
    output_dir: PathBuf,
}

impl ProtoC {
    fn new() -> Self {
        let (protoc_bin, protoc_includes) = protoc_prebuilt::init(PROTOC_VERSION).unwrap();
        Self {
            protoc_bin,
            includes: vec![protoc_includes],
            output_dir: PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("protobuf_generated"),
        }
    }

    fn include(mut self, path: impl Into<PathBuf>) -> Self {
        self.includes.push(path.into());
        self
    }

    fn generate_and_compile(
        &self,
        inputs: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.output_dir.exists() {
            let _ = std::fs::create_dir_all(&self.output_dir);
        }

        let mut cmd = Command::new(&self.protoc_bin);
        cmd.args(inputs)
            .arg(format!("--rust_out={}", self.output_dir.display()))
            .arg("--rust_opt=experimental-codegen=enabled,kernel=upb");
        for include in &self.includes {
            cmd.arg(format!("--proto_path={}", include.display()));
        }
        if !cmd.status()?.success() {
            return Err("protoc failed".into());
        }
        Ok(())
    }
}

fn main() {
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={}/{}", PROTO_BASE_DIR, proto);
    }

    ProtoC::new()
        .include(PROTO_BASE_DIR)
        .generate_and_compile(PROTOS)
        .expect("Failed to generate Rust code from proto files");
}
