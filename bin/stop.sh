#!/bin/bash

bin/hub-stop.sh
bin/transport-kinesis-stop.sh
docker network rm mercure_hub
