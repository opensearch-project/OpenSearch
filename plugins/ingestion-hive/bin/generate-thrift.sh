#!/bin/bash
#
# Regenerates Thrift client code from hive_metastore.thrift IDL.
# Uses Debian unstable's thrift-compiler (0.22.0) via Docker,
# matching the libthrift runtime version. Same approach as Trino's hive-thrift.
#
# Usage: ./bin/generate-thrift.sh

set -eu

cd "${BASH_SOURCE%/*}/.."

THRIFT_IDL="src/main/thrift/hive_metastore.thrift"
OUTPUT_DIR="src/main/java/org/opensearch/plugin/hive/metastore"
LICENSE_HEADER='/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

'

echo "Generating Thrift code from ${THRIFT_IDL}..."

rm -rf "${OUTPUT_DIR}"

docker run -v "${PWD}:/build" --rm debian:unstable /bin/sh -c "
set -eux
apt-get update -q
apt-get install -q -y thrift-compiler
mkdir -p /build/target/gen
thrift -o /build/target/gen \
  --gen java:private_members,sorted_containers,generated_annotations=suppress \
  /build/${THRIFT_IDL}
mv /build/target/gen/gen-java/org/opensearch/plugin/hive/metastore /build/${OUTPUT_DIR}
rm -rf /build/target
chown -R $(id -u) /build/src
"

echo "Adding license headers..."

for f in "${OUTPUT_DIR}"/*.java; do
  printf '%s' "${LICENSE_HEADER}" | cat - "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done

echo "Done. Generated $(ls "${OUTPUT_DIR}"/*.java | wc -l | tr -d ' ') files in ${OUTPUT_DIR}/"
echo "Running spotlessApply..."
cd "${BASH_SOURCE%/*}/.."
../../gradlew :plugins:ingestion-hive:spotlessApply -q
