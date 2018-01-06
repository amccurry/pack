#!/bin/bash
set -e
set -x

if [ -z ${1+x} ]; then
 echo "PACK_VERSION is not defined";
 exit 1
else
 echo "PACK_VERSION=${1}";
fi
PACK_VERSION=$1
docker build -t pack --build-arg PACK_VERSION=${PACK_VERSION} ../
