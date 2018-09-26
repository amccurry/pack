#!/bin/bash
set -ex

while true ; do

for I in {0..50} ; do
  dd if=/dev/urandom bs=1M count=10 2>/dev/null | tee /test/data${I}.data | md5sum > /test/data${I}.data.md5 &
done

wait

for I in {0..50} ; do
  EXPECTED=$(cat /test/data${I}.data.md5 | awk '{print $0}')
  ACTUAL=$(cat /test/data${I}.data | md5sum | awk '{print $0}')
  if [ "${ACTUAL}" != "${EXPECTED}" ] ; then
    echo "File md5 /test/data${I}.data does not match /test/data${I}.data.md5"
    exit 1
  fi
done

done
