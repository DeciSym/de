#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
RESOURCES_DIR="${SCRIPT_DIR}/../tests/resources"
TARGET_TTL="${RESOURCES_DIR}/superhero.ttl"
ZIP_FILE="${RESOURCES_DIR}/superhero-ttl.zip"
ZIP_URL="https://github.com/wallscope/superhero-rdf/raw/refs/heads/master/data/superhero-ttl.zip"

if [ -z "${CI:-}" ] && [ -f "${TARGET_TTL}" ]; then
    echo "dependencies present"
    exit 0
fi

mkdir -p "${RESOURCES_DIR}"

download_zip() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "${ZIP_URL}" -o "${ZIP_FILE}"
        return
    fi
    if command -v ftp >/dev/null 2>&1; then
        ftp -o "${ZIP_FILE}" "${ZIP_URL}"
        return
    fi
    if command -v wget >/dev/null 2>&1; then
        wget -O "${ZIP_FILE}" "${ZIP_URL}"
        return
    fi
    echo "error: need one of curl, ftp, or wget to download ${ZIP_URL}" >&2
    exit 1
}

ensure_unzip() {
    if command -v unzip >/dev/null 2>&1; then
        return
    fi

    # Keep legacy behavior on Debian/Ubuntu where apt-get is available.
    if command -v apt-get >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
        sudo apt-get install unzip -y
        return
    fi

    echo "error: 'unzip' is required but not installed" >&2
    exit 1
}

download_zip
ensure_unzip
unzip -o "${ZIP_FILE}" -d "${RESOURCES_DIR}"
rm -f "${ZIP_FILE}"
