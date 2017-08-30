#!/bin/bash
export CLASSPATH="/pack/hadoop-conf"
for f in /pack/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
echo $CLASSPATH
exec -a pack-compactor java -Xmx1g -Xms1g -cp ${CLASSPATH} pack.block.blockstore.compactor.PackCompactorServer
