#!/bin/bash

if [ ! -f ".env" ]
then
    cp .env.dist .env
fi

case $1 in
  dev)
    MERCURE_CADDY_FILE="Caddyfile.dev"
    ;;
  test)
    MERCURE_CADDY_FILE="Caddyfile.test"
    ;;
  *)
    MERCURE_CADDY_FILE="Caddyfile"
esac

export SERVER_NAME=`grep -oP "(?<=SERVER_NAME=).*" .env`
export MERCURE_TRANSPORT_URL=`grep -oP "(?<=MERCURE_TRANSPORT_URL=).*" .env`

./mercure adapt --config $MERCURE_CADDY_FILE > Caddyfile.json
jq -s '.[0] + .[1]' CaddyLoggingConfig.json Caddyfile.json > CaddyConfig.json
