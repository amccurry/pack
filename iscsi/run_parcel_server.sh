#!/bin/bash

PARCEL_FILE=$(ls iscsi-server/target/*.tar.gz)
PACK_VERSION=$(echo ${PARCEL_FILE##*/} | sed 's#PACK-##g' | sed 's#\.tar\.gz##g')
PROJECT_DIR="./target/http"

PARCEL_FILE_SHA=$PARCEL_FILE.sha
if command -v sha1sum ; then
  sha1sum $PARCEL_FILE | awk '{print $1}' > $PARCEL_FILE_SHA
else
  shasum $PARCEL_FILE | awk '{print $1}' > $PARCEL_FILE_SHA
fi
HASH=`cat $PARCEL_FILE_SHA`
LAST_UPDATED_SEC=`date +%s`
LAST_UPDATED="${LAST_UPDATED_SEC}0000"
HTTP_DIR="$PROJECT_DIR/http"
if [ -d $HTTP_DIR ]; then
  rm -r $HTTP_DIR
fi
mkdir -p $HTTP_DIR
MANIFEST="$HTTP_DIR/manifest.json"
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST
for DISTRO in el7
do
	if [ $DISTRO != "el7" ] ; then
		echo "," >> $MANIFEST
	fi
	DISTRO_PARCEL="PACK-${PACK_VERSION}-${DISTRO}.parcel"
	DISTRO_PARCEL_SHA="PACK-${PACK_VERSION}-${DISTRO}.parcel.sha"
	ln $PARCEL_FILE "${HTTP_DIR}/${DISTRO_PARCEL}"
	ln $PARCEL_FILE_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"PACK\",\"version\" : \"${PACK_VERSION}\",\"pkg_version\": \"${PACK_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
done
echo "]}" >> $MANIFEST

cd ${HTTP_DIR}
exec python -m SimpleHTTPServer 8001
