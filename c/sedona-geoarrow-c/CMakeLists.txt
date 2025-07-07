cmake_minimum_required(VERSION 3.14)
project(sedonadb_c_geoarrow_c)

# Note that this is not currently used by build.rs, it just exists to help
# IDEs understand the C sources
file(GLOB SOURCES src/**/*.c src/**/*.cc)
add_library(geoarrow_c ${SOURCES})
target_include_directories(geoarrow_c PRIVATE src)
target_compile_definitions(geoarrow_c PUBLIC GEOARROW_NAMESPACE=SedonaDB
                                             NANOARROW_NAMESPACE=SedonaDB)
