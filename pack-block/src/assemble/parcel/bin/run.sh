#!/bin/bash
set -e
set +x
export CLASSPATH="${HDFS_CONF_DIR}"
set +x
for f in ${PACK_PARCEL_PATH}/lib/*
do
  export CLASSPATH="${CLASSPATH}:${f}"
done
set -x
echo $CLASSPATH

if [ -z ${PACK_HDFS_KERBEROS_KEYTAB+x} ]; then
 echo "PACK_HDFS_KERBEROS_KEYTAB is not defined";
else
 echo "PACK_HDFS_KERBEROS_KEYTAB=${PACK_HDFS_KERBEROS_KEYTAB}";
fi

if [ -z ${PACK_HDFS_KERBEROS_PRINCIPAL_NAME+x} ]; then
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME is not defined";
else
 echo "PACK_HDFS_KERBEROS_PRINCIPAL_NAME=${PACK_HDFS_KERBEROS_PRINCIPAL_NAME}";
fi

if [ -z ${PACK_HDFS_PATH+x} ]; then
 echo "PACK_HDFS_PATH is not defined";
else
 echo "PACK_HDFS_PATH=${PACK_HDFS_PATH}";
fi

if [ -z ${PACK_HDFS_USER+x} ]; then
 echo "PACK_HDFS_USER is not defined";
else
 echo "PACK_HDFS_USER=${PACK_HDFS_USER}";
fi

if [ -z ${PACK_LOCAL+x} ]; then
 echo "PACK_LOCAL is not defined";
else
 echo "PACK_LOCAL=${PACK_LOCAL}";
fi

if [ -z ${PACK_SCOPE+x} ]; then
 echo "PACK_SCOPE is not defined";
else
 echo "PACK_SCOPE=${PACK_SCOPE}";
fi

exec -a pack java -Xmx1g -Xms1g -cp ${CLASSPATH} pack.block.server.BlockPackServer
