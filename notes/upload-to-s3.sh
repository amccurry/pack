#!/bin/bash
set -e

function usage() {
  echo "Usage:"
  echo "${0} --src|-s <source file> --bucket|-b <s3 bucket> --key|-k <s3 key> --accesskey|-a <access key> --secretkey|-s <secret key>"
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -s|--src)
    SRC_FILENAME="$2"
    shift # past argument
    shift # past value
    ;;
    -k|--key)
    KEY="$2"
    shift # past argument
    shift # past value
    ;;
    -b|--bucket)
    S3_BUCKET="$2"
    shift # past argument
    shift # past value
    ;;
    -a|--accesskey)
    S3_ACCESS_KEY="$2"
    shift # past argument
    shift # past value
    ;;
    -s|--secretkey)
    S3_SECRET_KEY="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ -z ${SRC_FILENAME+x} ]; then
  echo "--src is required";
  usage
  exit 1
fi

if [ -z ${KEY+x} ]; then
  echo "--key is required";
  usage
  exit 1
fi

if [ -z ${S3_BUCKET+x} ]; then
  echo "--bucket is required";
  usage
  exit 1
fi

if [ -z ${S3_ACCESS_KEY+x} ]; then
  echo "--accesskey is required";
  usage
  exit 1
fi

if [ -z ${S3_SECRET_KEY+x} ]; then
  echo "--secretkey is required";
  usage
  exit 1
fi

date=`date +%Y%m%d`
DATE_FORMATTED=`date -R`
RELATIVE_PATH="/${S3_BUCKET}/${KEY}"
CONTENT_TYPE="application/octet-stream"
STRING_TO_SIGN="PUT\n\n${CONTENT_TYPE}\n${DATE_FORMATTED}\n${RELATIVE_PATH}"
SIGNATURE=`echo -en ${STRING_TO_SIGN} | openssl sha1 -hmac ${S3_SECRET_KEY} -binary | base64`

time curl -X PUT -T "${SRC_FILENAME}" \
-H "Host: ${S3_BUCKET}.s3.amazonaws.com" \
-H "Date: ${DATE_FORMATTED}" \
-H "Content-Type: ${CONTENT_TYPE}" \
-H "Authorization: AWS ${S3_ACCESS_KEY}:${SIGNATURE}" \
http://${S3_BUCKET}.s3.amazonaws.com/${KEY}
