#!/bin/bash
set -e
set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

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
    exec -a pack-iscsi java -Xmx256m -Xms256m pack.iscsi.IscsiServerMain
    ;;
  (monitor)
    exec -a pack-monitor java -Xmx256m -Xms256m pack.distributed.storage.monitor.rpc.PackDistributedWriteBlockMonitorServer
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
