#!/bin/bash
set -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
THRIFT_DIR="$DIR/pack-volume-service/"
USER_ID=$(id -u)
exec docker run -u $USER_ID -it --rm -v "$THRIFT_DIR:/data" thrift thrift -out /data/src/main/java/ --gen java /data/src/thrift/volume-service.thrift
