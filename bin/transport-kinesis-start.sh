#!/bin/bash

export MERCURE_TRANSPORT_KINESIS_AWS_ACCESS_KEY_ID="DUMMYIDEXAMPLE"
export MERCURE_TRANSPORT_KINESIS_AWS_SECRET_ACCESS_KEY="DUMMYEXAMPLEKEY"
export MERCURE_TRANSPORT_KINESIS_AWS_REGION="eu-west-1"

docker run \
    --name mercure-transport \
    --network mercure_hub \
    -p 4567:4567 \
    -d saidsef/aws-kinesis-local:latest

aws configure set aws_access_key_id $MERCURE_TRANSPORT_KINESIS_AWS_ACCESS_KEY_ID --profile mercure-transport
aws configure set aws_secret_access_key $MERCURE_TRANSPORT_KINESIS_AWS_SECRET_ACCESS_KEY --profile mercure-transport
aws configure set region $MERCURE_TRANSPORT_KINESIS_AWS_REGION --profile mercure-transport

aws kinesis --endpoint-url http://localhost:4567 --profile mercure-transport create-stream --stream-name "mercure_updates" --shard-count 4
