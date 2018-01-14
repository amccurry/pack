#!/bin/bash
set -e
set -x

PACK_VERSION=$(ls -1 pack-assemble/target/PACK-*.dir/ | sed 's#PACK-##g')

docker build --build-arg PACK_VERSION=${PACK_VERSION} -q -t ${PLUGIN_NAME}:rootfs .
