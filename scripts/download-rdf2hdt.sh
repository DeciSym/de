#!/bin/bash

set +ex

deps_present=true

for i in rdf2hdt; do
    if ! command -v $i &> /dev/null; then
        deps_present=false
    fi
done

if [[ -z "${CI}" ]] && $deps_present; then
    echo "dependent binaries present"
    exit 0
fi

if [[ -z "${GITHUB_TOKEN}" ]]; then
   echo "Configure GITHUB_TOKEN environment variable: https://github.com/settings/tokens"
   exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GH_API="https://api.github.com"
GH_REPO="$GH_API/repos/DeciSym/mes"
GH_TAGS="$GH_REPO/releases/tags/v0.4"
AUTH="Authorization: token $GITHUB_TOKEN"
CURL_ARGS="-L"

curl -o /dev/null -sH "$AUTH" $GH_REPO || { echo "Error: Invalid repo, token or network issue!";  exit 1; }
# Read asset tags.
response=$(curl -sH "$AUTH" $GH_TAGS)
# Get ID of the asset based on given name.
id=$(echo "$response" | jq --arg name "rdf2hdt" '.assets[] | select(.name == $name).id')
[ "$id" ] || { echo "Error: Failed to get asset id, response: $response" | awk 'length($0)<100' >&2; exit 1; }
GH_ASSET="$GH_REPO/releases/assets/$id"
echo "Downloading asset..."
sudo curl $CURL_ARGS -H "$AUTH" -H 'Accept: application/octet-stream' "$GH_ASSET" -o /usr/bin/rdf2hdt
sudo chmod +x /usr/bin/rdf2hdt
# make sure binary is available for packaging
sudo cp /usr/bin/rdf2hdt $SCRIPT_DIR/../deps
