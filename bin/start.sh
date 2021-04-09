#!/bin/bash

if [ ! -f ".env" ]
then
    echo "------------------------------------------------------------"
    echo "| Creating .env file                                       |"
    echo "------------------------------------------------------------"
    cp .env.dist .env
fi

echo ""
echo "------------------------------------------------------------"
echo "| Stopping mercure-hub                                     |"
echo "------------------------------------------------------------"
bin/stop.sh

echo ""
echo "------------------------------------------------------------"
echo "| Creating mercure_hub network                             |"
echo "------------------------------------------------------------"
docker network create mercure_hub --driver bridge

echo ""
echo "------------------------------------------------------------"
echo "| Building Kinesis container                               |"
echo "------------------------------------------------------------"
bin/transport-kinesis-start.sh

echo ""
echo "------------------------------------------------------------"
echo "| Building mercure-hub container                           |"
echo "------------------------------------------------------------"
bin/hub-start.sh

if [ "$2" ]
    then
    echo ""
    echo "------------------------------------------------------------"
    echo "| Connecting mercure-hub to network                        |"
    echo "------------------------------------------------------------"
    docker network connect $2 mercure-hub
fi

echo ""
echo "------------------------------------------------------------"
echo "| Finished                                                 |"
echo "------------------------------------------------------------"
echo ""
echo "Stop and destroy mercure-hub:"
echo "./bin/stop.sh"
echo ""
echo "Dump current output to file:"
echo "docker logs mercure-hub >& mercure-hub.log"
echo ""
