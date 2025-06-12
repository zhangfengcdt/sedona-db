
main() {
  local -r repo_url="https://github.com/apache/arrow-nanoarrow"
  # Check releases page: https://github.com/apache/arrow-nanoarrow/releases/
  local -r commit_sha=8c4e869cc8e4920737a513bc3012780050016bc5

  echo "Fetching $commit_sha from $repo_url"
  SCRATCH=$(mktemp -d)
  trap 'rm -rf "$SCRATCH"' EXIT

  local -r tarball="$SCRATCH/nanoarrow.tar.gz"
  wget -O "$tarball" "$repo_url/archive/$commit_sha.tar.gz"
  tar --strip-components 1 -C "$SCRATCH" -xf "$tarball"

  # Remove previous bundle
  rm -rf src/nanoarrow

  # Build the bundle
  python3 "${SCRATCH}/ci/scripts/bundle.py" \
      --include-output-dir=src \
      --source-output-dir=src/nanoarrow
}

main
