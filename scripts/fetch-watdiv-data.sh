#!/bin/sh
#
# Fetch the WatDiv fixtures the `watdiv` benchmark target reads.
#
# The archives are the ones Comunica publishes for its own HDT benchmarks
# (comunica/comunica-performance-assets). Each one holds an already-built
# `dataset.hdt` plus twenty WatDiv query-template files with five parameterised
# instances each -- the same 5 x 20 workload Comunica's `benchmark-watdiv-hdt`
# runs. Reusing their assets rather than running the WatDiv generator ourselves
# keeps the two engines' numbers comparable and keeps Java off the build path.
#
# The download is pinned to one commit of that repository and checked against
# the SHA-256 its Git LFS pointer records, so the fixtures can't change under
# the benchmarks.
#
# Only `dataset.hdt` and `queries/` are extracted. The archives also carry
# `dataset.nt` (152 MB at scale 10, ~1.5 GB at scale 100) and an hdt-cpp
# `.index.v1-1` sidecar; `de` needs neither, and `hdt` builds its own
# `.index.v5-rust-cache` on first use.
#
# Usage: scripts/fetch-watdiv-data.sh [10|100]

set -eu

SCALE="${1:-10}"

# The commit that added scale 100 (2026-09-09). Scale 10's archive is
# unchanged there since 2024-05-30.
ASSETS_COMMIT=9a444ab214ef1dc23ea323dbc77a8ec6a41cd5eb

case "${SCALE}" in
    10) SHA256=3a9876a5d62e1f2bc0ab7b6f188e9ee66658a0b1d6600e82b03dd85453f35189 ;;
    100) SHA256=1731820db2a200f6d04e8c1c1bb42ac624fabde6a43d485bc1eeda5294651843 ;;
    *)
        echo "error: scale must be 10 or 100, got '${SCALE}'" >&2
        exit 1
        ;;
esac

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
DEST="${SCRIPT_DIR}/../benches/watdiv/${SCALE}"
ZIP_URL="https://github.com/comunica/comunica-performance-assets/raw/${ASSETS_COMMIT}/watdiv-${SCALE}.zip"

if [ -f "${DEST}/dataset.hdt" ] && [ -d "${DEST}/queries" ]; then
    echo "watdiv-${SCALE} fixtures present at ${DEST}"
    exit 0
fi

for tool in curl unzip; do
    command -v "${tool}" >/dev/null 2>&1 || {
        echo "error: '${tool}' is required to fetch the WatDiv fixtures" >&2
        exit 1
    }
done

# sha256sum on Linux, shasum on macOS
verify() {
    if command -v sha256sum >/dev/null 2>&1; then
        echo "${SHA256}  $1" | sha256sum -c - >/dev/null
    else
        echo "${SHA256}  $1" | shasum -a 256 -c - >/dev/null
    fi
}

mkdir -p "${DEST}"
ZIP_FILE="${DEST}/watdiv-${SCALE}.zip"

echo "downloading watdiv-${SCALE} (15 MB at scale 10, 164 MB at scale 100)"
curl -fsSL "${ZIP_URL}" -o "${ZIP_FILE}"
if ! verify "${ZIP_FILE}"; then
    echo "error: ${ZIP_URL} doesn't match SHA-256 ${SHA256}" >&2
    rm -f "${ZIP_FILE}"
    exit 1
fi

echo "extracting dataset.hdt and queries/ into ${DEST}"
unzip -o -q "${ZIP_FILE}" 'dataset.hdt' 'queries/*' -d "${DEST}"
rm -f "${ZIP_FILE}"

echo "watdiv-${SCALE} ready:"
echo "  $(du -h "${DEST}/dataset.hdt" | cut -f1)  ${DEST}/dataset.hdt"
echo "  $(ls "${DEST}/queries" | wc -l | tr -d ' ') query templates in ${DEST}/queries"
