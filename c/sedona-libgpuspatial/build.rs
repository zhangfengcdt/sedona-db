use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=libgpuspatial");
    println!("cargo::rustc-check-cfg=cfg(gpu_available)");

    // Check if gpu feature is enabled
    let gpu_feature_enabled = env::var("CARGO_FEATURE_GPU").is_ok();

    if !gpu_feature_enabled {
        println!(
            "cargo:warning=GPU feature not enabled. Use --features gpu to enable GPU support."
        );
        // Create empty bindings file so the build doesn't fail
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::write(out_path.join("bindings.rs"), "// GPU feature not enabled\n")
            .expect("Couldn't write empty bindings!");
        return;
    }

    // Check if libgpuspatial submodule exists
    let libgpuspatial_path = std::path::Path::new("./libgpuspatial/CMakeLists.txt");
    if !libgpuspatial_path.exists() {
        println!("cargo:warning=libgpuspatial submodule not found. GPU functionality will not be available.");
        println!("cargo:warning=To enable GPU support, initialize the submodule: git submodule update --init --recursive");

        // Create empty bindings file so the build doesn't fail
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::write(
            out_path.join("bindings.rs"),
            "// libgpuspatial submodule not available\n",
        )
        .expect("Couldn't write empty bindings!");
        return;
    }

    // Check if CUDA is available
    let cuda_available = env::var("CUDA_HOME").is_ok()
        || std::path::Path::new("/usr/local/cuda").exists()
        || which::which("nvcc").is_ok();

    if cuda_available {
        // Compile the library for A10 (86), L4, L40 (89) GPUs
        // You should adjust this based on your target GPUs
        // Otherwise, it calls JIT compilation which has a startup overhead

        let dst = cmake::Config::new("./libgpuspatial")
            .define("CMAKE_CUDA_ARCHITECTURES", "86")
            .define("CMAKE_POLICY_VERSION_MINIMUM", "3.5")  // Allow older CMake versions
            .build();
        let include_path = dst.join("include");
        println!(
            "cargo:rustc-link-search=native={}",
            dst.join("lib").display()
        ); // Link to the cmake output lib directory

        // Link to the static libraries and CUDA runtime
        println!("cargo:rustc-link-search=native={}/build", dst.display()); // gpuspatial_c defined in CMakeLists.txt

        // Detect CUDA library path from CUDA_HOME or default locations
        let cuda_lib_path = if let Ok(cuda_home) = env::var("CUDA_HOME") {
            format!("{}/lib64", cuda_home)
        } else if std::path::Path::new("/usr/local/cuda-12.4/lib64").exists() {
            "/usr/local/cuda-12.4/lib64".to_string()
        } else if std::path::Path::new("/usr/local/cuda/lib64").exists() {
            "/usr/local/cuda/lib64".to_string()
        } else {
            "/usr/local/cuda/lib64".to_string() // fallback
        };

        println!("cargo:rustc-link-search=native={}", cuda_lib_path); // CUDA runtime
        println!("cargo:rustc-link-search=native=/usr/lib/x86_64-linux-gnu"); // CUDA Driver (alternative location)

        println!("cargo:rustc-link-lib=static=gpuspatial_c");
        println!("cargo:rustc-link-lib=static=gpuspatial");
        println!("cargo:rustc-link-lib=static=rmm");
        println!("cargo:rustc-link-lib=static=geoarrow");
        println!("cargo:rustc-link-lib=static=nanoarrow_static");
        println!("cargo:rustc-link-lib=stdc++");
        println!("cargo:rustc-link-lib=dylib=cudart"); // Link to the CUDA runtime dynamically
        println!("cargo:rustc-link-lib=dylib=cuda"); // Link to the CUDA driver library dynamically

        // Generate bindings from the header
        let bindings = bindgen::Builder::default()
            .header(
                include_path
                    .join("gpuspatial/gpuspatial_c.h")
                    .to_str()
                    .unwrap(),
            )
            .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
            .generate()
            .expect("Unable to generate bindings");

        // Write the bindings to the $OUT_DIR/bindings.rs file.
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out_path.join("bindings.rs"))
            .expect("Couldn't write bindings!");

        println!("cargo:rustc-cfg=gpu_available");
    } else {
        println!("cargo:warning=CUDA not found. GPU functionality will not be available.");
        println!("cargo:warning=Install CUDA and set CUDA_HOME to enable GPU support.");

        // Create empty bindings file so the build doesn't fail
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::write(out_path.join("bindings.rs"), "// CUDA not available\n")
            .expect("Couldn't write empty bindings!");
    }
}
