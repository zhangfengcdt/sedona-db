cmake_minimum_required(VERSION 3.14)
project(sedonadb_c)

# This top-level project is just a way to help IDEs understand the C sources
# individual projects are responsible for building their own static libraries
# (which may or may not invoke cmake).
add_subdirectory(sedona-geoarrow-c)
add_subdirectory(sedona-s2geography)
