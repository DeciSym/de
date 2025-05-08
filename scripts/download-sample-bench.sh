#!/bin/bash

set +ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [[ -z "${CI}" ]] && [[ -f "${SCRIPT_DIR}/../tests/resources/superhero.ttl" ]]; then
    echo "dependencies present"
    exit 0
fi

curl -L  https://github.com/wallscope/superhero-rdf/raw/refs/heads/master/data/superhero-ttl.zip -o $SCRIPT_DIR/../tests/resources/superhero-ttl.zip
sudo apt-get install unzip -y
unzip -o $SCRIPT_DIR/../tests/resources/superhero-ttl.zip -d $SCRIPT_DIR/../tests/resources/
