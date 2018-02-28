#!/bin/bash
set -e
set -x

set +x
export CLASSPATH="${HDFS_CONF_DIR}"
set +x
for f in ${PACK_PARCEL_PATH}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
for f in ${PACK_PARCEL_PATH}/lib/jars/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
set -x

exec -a pack-iscsi java -Xmx1g -Xms1g ${JVM_OPTIONS} pack.iscsi.IscsiServerMain "$@"
