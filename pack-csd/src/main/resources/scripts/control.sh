#!/bin/bash
set -e
set -x

mkdir -p /var/log/pack

export HDFS_CONF_DIR="${PWD}/hadoop-conf/"
export PACK_LOG4J_CONFIG="${PWD}/log4j.properties"

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 export PACK_HDFS_USER=hdfs
 export HADOOP_USER_NAME=hdfs
else
 export PACK_HDFS_KERBEROS_KEYTAB="${PWD}/pack.keytab"
 export PACK_HDFS_KERBEROS_PRINCIPAL_NAME="${hdfs_principal}"
fi

CMD=$1

case $CMD in
  (agent)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh agent
    ;;
  (compactor)
    exec -a pack ${PACK_PARCEL_PATH}/bin/run.sh compactor
    ;;
  (gateway)
    exec -a "gateway" sed -e 's/^/export /' ${PWD}/../pack.properties > ${PWD}/pack.properties
    ;;
  (chown)
    if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
      exec -a chown hdfs --config ${PWD}/hadoop-conf/ dfs -chown -R hdfs:hdfs ${PACK_HDFS_PATH}
    else
      TKT_FILE=$(mktemp)
      kinit -kt ${PACK_HDFS_KERBEROS_KEYTAB} -c ${TKT_FILE} ${PACK_HDFS_KERBEROS_PRINCIPAL_NAME}
      export KRB5CCNAME=${TKT_FILE}
      exec -a chown hdfs --config ${PWD}/hadoop-conf/ dfs -chown -R hdfs:hdfs ${PACK_HDFS_PATH}
    fi
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac
