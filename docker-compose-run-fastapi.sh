#!/bin/bash
source ./.env

cp ./build/fastapi/dockerfile .
cp ./build/fastapi/docker-compose.yaml .
docker compose down -v && docker compose up --build 
rm -fr ./docker-compose.yaml
rm -fr ./dockerfile