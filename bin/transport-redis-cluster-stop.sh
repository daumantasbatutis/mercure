#!/bin/bash

docker kill mercure-transport-1
docker rm mercure-transport-1

docker kill mercure-transport-2
docker rm mercure-transport-2

docker kill mercure-transport-3
docker rm mercure-transport-3

docker kill mercure-transport-init
docker rm mercure-transport-init
