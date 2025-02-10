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
sudo curl -L -H "Accept: application/octet-stream" -H "Authorization: Bearer $GITHUB_TOKEN" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/DeciSym/mes/releases/assets/176098984 -o /usr/bin/rdf2hdt
sudo chmod +x /usr/bin/rdf2hdt
# make sure binary is available for packaging
sudo cp /usr/bin/rdf2hdt $SCRIPT_DIR/../deps