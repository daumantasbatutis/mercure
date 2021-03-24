#!/bin/bash

./xcaddy build \
    --with github.com/dunglas/mercure/caddy=$PWD/caddy \
    --with github.com/dunglas/mercure=$PWD
mv caddy/caddy mercure
