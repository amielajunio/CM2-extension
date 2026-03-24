#!/bin/bash

# Funzione per trovare il nome reale del container anche con prefissi
get_name() {
    docker ps --format "{{.Names}}" | grep "$1" | head -n 1
}

CONF=$(get_name "configsvr")
SHARD=$(get_name "shard1")
MONGOS=$(get_name "mongos")

echo "Inizializzo Config Server su: $CONF"
docker exec -it $CONF mongosh --eval 'rs.initiate({_id: "rs-config", configsvr: true, members: [{_id: 0, host: "configsvr:27017"}]})'

echo "Inizializzo Shard su: $SHARD"
docker exec -it $SHARD mongosh --eval 'rs.initiate({_id: "rs-shard1", members: [{_id: 0, host: "shard1:27017"}]})'

echo "Aspetto il setup dei set (10s)..."
sleep 10

echo "Aggiungo lo Shard al router: $MONGOS"
docker exec -it $MONGOS mongosh --eval 'sh.addShard("rs-shard1/shard1:27017")'