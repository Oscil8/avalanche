#!/bin/bash
# Cleanup old images
IMAGE_IDS=$(docker images | grep ^quay.io/freshtracks.io/avalanche | tr -s " " | cut -d " " -f3 )
if [ -n "$IMAGE_IDS" ]; then
  docker rmi -f $IMAGE_IDS
fi
# Build local images and quay
make
# Upload new images
docker login -u="$ARTIFACTORY_USERNAME" -p="$ARTIFACTORY_PASSWORD" docker.internal.sysdig.com
IMAGE_ID=$(docker images | grep ^quay.io/freshtracks.io/avalanche | tr -s " " | cut -d " " -f3 )
echo found $IMAGE_ID
docker tag ${IMAGE_ID} docker.internal.sysdig.com/avalanche:goldman-beacon-10
docker push docker.internal.sysdig.com/avalanche:goldman-beacon-10
