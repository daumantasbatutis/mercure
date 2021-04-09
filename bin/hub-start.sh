#!/bin/bash

docker build -f Dockerfile.dev -t mercure-hub ./

docker run \
    --name mercure-hub \
    --network mercure_hub \
    --env-file .env \
    -p 9080:80 \
    -p 9443:443 \
    -d mercure-hub caddy run --config /etc/caddy/CaddyConfig.json
