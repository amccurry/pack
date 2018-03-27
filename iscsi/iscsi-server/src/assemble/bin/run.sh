#!/bin/bash
set -e
set -x

if [ -z ${PACK_PARCEL+x} ] ; then
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
else
  DIR="${PACK_PARCEL}"
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
  (volume-manager)
    if [ -z ${DOCKER_PLUGIN_SOCK_PATH+x} ] ; then
      export DOCKER_PLUGIN_SOCK_PATH="/var/lib/pack/pack.sock"
    fi

    if [ "$(dirname $DOCKER_PLUGIN_SOCK_PATH)" != "/run/docker/plugins" ] ; then
      if ! sudo mkdir -p /etc/docker/plugins/ ; then
        echo "WARNING: Cannot sudo to write spec file out for docker."
      fi
      if ! sudo echo "unix://${DOCKER_PLUGIN_SOCK_PATH}" > /etc/docker/plugins/pack.spec ; then
        echo "WARNING: Cannot sudo to write spec file out for docker."
      fi
    fi
    exec -a pack-volume-manager java -Xmx64m -Xms64m $JAVA_OPTIONS pack.iscsi.docker.DockerVolumePluginServerMain
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
