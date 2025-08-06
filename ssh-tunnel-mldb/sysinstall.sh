#!/bin/bash

KEY_PATH='/Users/vetybhakti/Documents/Renos/private_key/vety_lestari_renos_id.pem'
cp ${KEY_PATH} ./key.pem
docker compose down -v 
docker compose up -d --build
rm -fr ./key.pem