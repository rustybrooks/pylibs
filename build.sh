#!/bin/bash

set -x

TAG="latest"
LIBS="api-framework sqllib cachelib configlib"

if [ "$1" != "" ]; then
    LIBS=$1
fi
echo $LIBS

find . -name target -exec rm -rf \{\} \;

docker build -t pylibs-builder:$TAG .

for LIB in $LIBS; do
    # You can use -X for a more verbose output (includes coverage lines)
    docker-compose run -w /pylibs/$LIB \
                       pylibs-builder-test \
                       pyb install_dependencies publish -v
done

docker-compose down -v --rmi local --remove-orphans
# docker rmi pylibs-builder:$TAG

find . -name '*.whl' | grep -v "/builds/"