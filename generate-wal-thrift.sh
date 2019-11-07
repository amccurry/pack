#!/bin/bash
set -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WAL_DIR="$DIR/pack-storage-modules/wal/pack-remote-wal/"
USER_ID=$(id -u)
exec docker run -u $USER_ID -it --rm -v "$WAL_DIR:/data" thrift thrift -out /data/src/main/java/ --gen java /data/src/thrift/remote-wal.thrift
