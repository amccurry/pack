#!/bin/bash
export CLASSPATH="/pack/hadoop-conf"
for f in /pack/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
echo $CLASSPATH
exec -a pack java -Xmx64m -Xms64m -cp ${CLASSPATH} pack.TarPackServer
