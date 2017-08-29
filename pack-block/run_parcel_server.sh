#!/bin/bash

set +x

BASE_PROJECT_DIR=`dirname "$0"`
BASE_PROJECT_DIR=`cd "$BASE_PROJECT_DIR"; pwd`

MAVEN_ARGS="$@"


function buildHttpDir {
  PROJECT_DIR=$1
  cd "$PROJECT_DIR"

  TARGET="${PROJECT_DIR}/target"

  PARCEL_NAME=$(mvn help:evaluate $MAVEN_ARGS -Dexpression=parcel.name | grep -Ev '(^\[|Download\w+:)')

  LONG_FILENAME=$(ls -1 ${TARGET}/${PARCEL_NAME}-*.tar.gz)
  FILENAME=$(basename ${LONG_FILENAME})

  PARCEL_VERSION=$(echo ${FILENAME} | sed -e "s/${PARCEL_NAME}-//g" | sed -e 's/.tar.gz//g')
  echo "PARCEL_VERSION=${PARCEL_VERSION}"


  PARCEL="${TARGET}/${PARCEL_NAME}-${PARCEL_VERSION}.tar.gz"
  PARCEL_SHA="${PARCEL}.sha"

  if hash sha1sum 2>/dev/null; then
    sha1sum $PARCEL | awk '{print $1}' > $PARCEL_SHA
  else
    shasum $PARCEL | awk '{print $1}' > $PARCEL_SHA
  fi

  HASH=`cat $PARCEL_SHA`
  for DISTRO in el5 el6 el7 sles11 lucid precise trusty squeeze wheezy
  do
  	if [ $DISTRO != "el5" ] ; then
	  	echo "," >> $MANIFEST
	  fi
	  DISTRO_PARCEL="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel"
	  DISTRO_PARCEL_SHA="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel.sha"
	  ln $PARCEL "${HTTP_DIR}/${DISTRO_PARCEL}"
	  ln $PARCEL_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	  echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"${PARCEL_NAME}\",\"version\" : \"${PARCEL_VERSION}\",\"pkg_version\": \"${PARCEL_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
  done
}

cd "$BASE_PROJECT_DIR"
HTTP_DIR="$BASE_PROJECT_DIR/target/tmp-http"
rm -r $HTTP_DIR
mkdir -p $HTTP_DIR

LAST_UPDATED_SEC=$(date +%s)
LAST_UPDATED="${LAST_UPDATED_SEC}0000"

MANIFEST="${HTTP_DIR}/manifest.json"
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST

buildHttpDir $BASE_PROJECT_DIR

echo "]}" >> $MANIFEST
cd ${HTTP_DIR}
python -m SimpleHTTPServer
