#!/bin/bash

docker run \
    --name mercure-transport-1 \
    --network mercure_hub \
    -e ALLOW_EMPTY_PASSWORD=yes \
    -e REDIS_NODES="mercure-transport-1 mercure-transport-2 mercure-transport-3" \
    -p 9379:6379 \
    -d bitnami/redis-cluster:latest

docker run \
    --name mercure-transport-2 \
    --network mercure_hub \
    -e ALLOW_EMPTY_PASSWORD=yes \
    -e REDIS_NODES="mercure-transport-1 mercure-transport-2 mercure-transport-3" \
    -p 9380:6379 \
    -d bitnami/redis-cluster:latest

docker run \
    --name mercure-transport-3 \
    --network mercure_hub \
    -e ALLOW_EMPTY_PASSWORD=yes \
    -e REDIS_NODES="mercure-transport-1 mercure-transport-2 mercure-transport-3" \
    -p 9381:6379 \
    -d bitnami/redis-cluster:latest

docker run \
    --name mercure-transport-init \
    --network mercure_hub \
    -e REDIS_CLUSTER_REPLICAS=0 \
    -e REDIS_CLUSTER_CREATOR=yes \
    -e REDIS_NODES="mercure-transport-1 mercure-transport-2 mercure-transport-3" \
    -p 9382:6379 \
    bitnami/redis-cluster:latest
