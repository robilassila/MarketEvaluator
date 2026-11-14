#!/usr/bin/env bash
# Downloads the DEBS 2022 Grand Challenge trading data (one CSV per day, ~25 GB in total)
# from Zenodo, verifies each file's MD5, and checks that it contains no quote characters,
# which the parser rejects. Re-running skips verified files and resumes partial downloads.
#
# Usage: scripts/download-data.sh [target-directory]   (default: data/ in the repository)
set -euo pipefail

RECORD_URL="https://zenodo.org/api/records/6382482/files"
TARGET="${1:-"$(cd "$(dirname "$0")/.." && pwd)/data"}"

# file name, MD5 (from the Zenodo record)
FILES=(
  "debs2022-gc-trading-day-08-11-21.csv a08013f09a6b77355dae8b3ce141624d"
  "debs2022-gc-trading-day-09-11-21.csv 3e1b7a79c63c71e03f4cef82fbda94a2"
  "debs2022-gc-trading-day-10-11-21.csv bed2f79e124f68da54929500f115a820"
  "debs2022-gc-trading-day-11-11-21.csv 943b8b04853c3fe70477b5aa1e5c6533"
  "debs2022-gc-trading-day-12-11-21.csv 4d9d0529948879e581cbe047eb23bfb7"
  "debs2022-gc-trading-day-13-11-21.csv 64f57410de31c5753b848bf4eeee7bc4"
  "debs2022-gc-trading-day-14-11-21.csv adfc75f72d9348209d602baa72af44b5"
)

md5_of() {
  if command -v md5sum >/dev/null; then
    md5sum "$1" | cut -d' ' -f1
  else
    md5 -q "$1"
  fi
}

mkdir -p "$TARGET"
failed=0

for entry in "${FILES[@]}"; do
  read -r name expected <<<"$entry"
  path="$TARGET/$name"

  if [[ -f "$path" ]]; then
    echo "Verifying existing $name"
  else
    echo "Downloading $name"
    # Partial downloads stay in .part and are resumed with -C - on the next run.
    curl --fail --location --retry 5 --continue-at - \
      --output "$path.part" "$RECORD_URL/$name/content"
    mv "$path.part" "$path"
  fi

  actual="$(md5_of "$path")"
  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: $name MD5 is $actual, expected $expected. Delete it and re-run." >&2
    failed=1
    continue
  fi

  quotes="$(grep -c '"' "$path" || true)"
  if [[ "$quotes" != 0 ]]; then
    echo "WARNING: $name has $quotes lines containing '\"'; the parser will reject this file." >&2
    failed=1
  fi
  echo "OK $name"
done

exit "$failed"
