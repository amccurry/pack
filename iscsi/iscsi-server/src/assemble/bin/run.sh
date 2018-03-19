#!/bin/bash
set -e
set -x

if [ -z ${PACK_ISCSI_PARCEL+x} ] ; then
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
else
  DIR="${PACK_ISCSI_PARCEL}"
fi

set +x
for f in ${DIR}/jars/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
for f in ${DIR}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
set -x

CMD=$1

case $CMD in
  (iscsi)
    exec -a pack-iscsi java  -Xmx1g -Xms1g $JAVA_OPTIONS pack.iscsi.IscsiServerMain
    ;;
  (compactor)
    exec -a pack-compactor java -Xmx1g -Xms1g $JAVA_OPTIONS pack.distributed.storage.compactor.PackCompactorServer
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
