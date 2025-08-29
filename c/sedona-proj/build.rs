use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=src/proj_dyn.c");
    cc::Build::new().file("src/proj_dyn.c").compile("proj_dyn");

    let bindings = bindgen::Builder::default()
        .header("src/proj_dyn.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
