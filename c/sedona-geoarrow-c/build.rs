use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=src/geoarrow/double_parse_fast_float.cc");
    println!("cargo:rerun-if-changed=src/geoarrow/geoarrow.c");
    println!("cargo:rerun-if-changed=src/nanoarrow/nanoarrow.c");

    cc::Build::new()
        .file("src/geoarrow/ryu/d2s.c")
        .file("src/geoarrow/geoarrow.c")
        .file("src/nanoarrow/nanoarrow.c")
        .include("src/")
        .flag("-DGEOARROW_NAMESPACE=SedonaDB")
        .flag("-DNANOARROW_NAMESPACE=SedonaDB")
        .compile("geoarrow");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("src/geoarrow/double_parse_fast_float.cc")
        .include("src/")
        .flag("-DGEOARROW_NAMESPACE=SedonaDB")
        .flag("-DNANOARROW_NAMESPACE=SedonaDB")
        .compile("geoarrow_fast_float");

    let bindings = bindgen::Builder::default()
        .header("src/geoarrow/geoarrow.h")
        .clang_arg("-DGEOARROW_NAMESPACE=SedonaDB")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
