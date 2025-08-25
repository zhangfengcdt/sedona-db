
# geos dynamic linking workaround for Windows

This directory contains a small workaround for the current geos crate for Windows, whereby the build.rs script can't dynaically link GEOS without pkg-config AND geos-config (even though these typically don't exist in a Windows MSVC build of GEOS). These files are two executables that print the required output to make the build script happy for the pinned version of geos.

See <https://github.com/georust/geos/issues/208> for follow-up discussion/to check if this constraint was removed when bumping a GEOS version.
