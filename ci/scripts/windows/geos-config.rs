fn main() {
    // The first line is a -Lpath/to/ that contains geos_c.dll
    if let Ok(lib_dir) = std::env::var("GEOS_LIB_DIR") {
        println!("-L{}", lib_dir);
    } else {
        eprintln!("GEOS_LIB_DIR environment variable not set");
        std::process::exit(1);
    }

    // The second line is the GEOS version
    if let Ok(version) = std::env::var("GEOS_VERSION") {
        println!("{}", version);
    } else {
        eprintln!("GEOS_VERSION environment variable not set");
        std::process::exit(1);
    }

    std::process::exit(0);
}
