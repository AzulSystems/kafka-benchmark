#!/bin/bash

KAFKA_BASE_DIR=$(cd $(dirname $0); pwd)

bash "${KAFKA_BASE_DIR}/../benchmarks-common/build.sh"

cd "${KAFKA_BASE_DIR}" || exit 1
echo "Building $(pwd)..."

rm -v *.jar

mvn clean package -DskipTests || exit 1

from=$(find target -name *-jar-with-dependencies.jar)
to=${from/-jar-with-dependencies/}
to=${to##*/}

cp -fv ${from} ${to}
chmod 777 ${to}
