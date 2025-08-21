#!/usr/bin/env bash
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SEDONADB_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

if [ -z "$VCPKG_ROOT" ] || [ -z "$VCPKG_DEFAULT_TRIPLET" ]; then
    echo "wheels-bootstrap-vcpkg.sh requires environment variable VCPKG_ROOT to be set"
    echo "wheels-bootstrap-vcpkg.sh requires environment variable VCPKG_DEFAULT_TRIPLET to be set"
    exit 1
else
    export VCPKG_INSTALL_NAME_DIR="${VCPKG_ROOT}/installed/${VCPKG_DEFAULT_TRIPLET}/lib"

    # Ensure that geos-config from vcpkg is picked up before system geos-config
    # (e.g., from Homebrew)
    export PATH="${VCPKG_ROOT}/installed/${VCPKG_DEFAULT_TRIPLET}/tools/geos/bin:${PATH}"

    pushd ${VCPKG_ROOT}
    ./bootstrap-vcpkg.sh
    ./vcpkg install --overlay-triplets="${SEDONADB_DIR}/ci/scripts/custom-triplets" geos
    popd

    export CMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
    export PKG_CONFIG_PATH=${VCPKG_ROOT}/installed/${VCPKG_DEFAULT_TRIPLET}/lib/pkgconfig:$PKG_CONFIG_PATH
fi
