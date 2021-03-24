#!/bin/bash

docker build -t mercure-hub ./

echo $MERCURE_CADDY_FILE

docker run \
    --name mercure-hub \
    --network mercure_hub \
    -e MERCURE_PUBLISHER_JWT_KEY='!ChangeMe!' \
    -e MERCURE_SUBSCRIBER_JWT_KEY='!ChangeMe!' \
    -e MERCURE_TRANSPORT_URL=$MERCURE_TRANSPORT_URL \
    -e SERVER_NAME=':80' \
    -p 9080:80 \
    -p 9443:443 \
    -d mercure-hub caddy run -config /etc/caddy/$MERCURE_CADDY_FILE

docker network connect marketplace_net mercure-hub

docker logs -f mercure-hub >& log/caddy.log &
