# sedonadb

SedonaDB is a high-performance, dependency-free geospatial compute engine.

## Development

SedonaDB is written and Rust and is a standard `cargo` workspace. You can
install a recent version of the Rust compiler and cargo from
[rustup.rs](https://rustup.rs/) and run tests using `cargo test`.

Some tests require submodules that contain test data or pinned versions of
external dependencies. These submodules can be initialized with:

```shell
git submodule init
git submodule update --recursive
```

Some crates wrap external native libraries and require system dependencies
to build. At this time the only crate that requires this is the sedona-s2geography
crate, which requires [CMake](https://cmake.org),
[Abseil](https://github.com/abseil/abseil-cpp) and OpenSSL. These can be installed
on MacOS with [Homebrew](https://brew.sh):

```shell
brew install abseil openssl cmake
```

On Linux and Windows, it is recommended to use [vcpkg](https://github.com/microsoft/vcpkg)
to provide external dependencies. This can be done by setting the `CMAKE_TOOLCHAIN_FILE`
environment variable:

```shell
export CMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

When using VSCode, it may be necessary to set this environment variable in settings.json
such that it can be found by rust-analyzer when running build/run tasks:

```json
{
    "rust-analyzer.runnables.extraEnv": {
        "CMAKE_TOOLCHAIN_FILE": "/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake"
    },
    "rust-analyzer.cargo.extraEnv": {
        "CMAKE_TOOLCHAIN_FILE": "/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake"
    }
}
```
