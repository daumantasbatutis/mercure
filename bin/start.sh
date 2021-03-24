#!/bin/bash

TRANSPORT=$1

if [ -z "$TRANSPORT" ]
then
  echo "Which transport to use (redis-cluster/bolt):"
  read
fi

case $2 in
  prod)
    export MERCURE_CADDY_FILE="Caddyfile"
    ;;
  test)
    export MERCURE_CADDY_FILE="Caddyfile.test"
    ;;
  *)
    export MERCURE_CADDY_FILE="Caddyfile.dev"
esac

case $TRANSPORT in
  bolt)
    export MERCURE_TRANSPORT_URL="bolt://mercure.db"
    ;;
  redis-cluster)
    export MERCURE_TRANSPORT_URL="redis://mercure-transport-1:6379,mercure-transport-2:6379,mercure-transport-3:6379/0?stream_count=20"
    ;;
  *)
    echo "Transport $TRANSPORT not supported!"
    exit 1
esac

docker network create mercure_hub --driver bridge

bin/stop.sh

bin/compile.sh

if test -f "bin/transport-$TRANSPORT-start.sh"
then
    bin/transport-$TRANSPORT-start.sh
fi

bin/hub-start.sh
