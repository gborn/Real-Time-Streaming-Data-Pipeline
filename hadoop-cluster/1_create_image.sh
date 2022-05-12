#!/bin/bash

# generate ssh key
echo "Y" | ssh-keygen -t rsa -P "" -f configs/id_rsa

# Building Hadoop Docker Image
docker build -f ./hadoop/Dockerfile . -t cluster:hadoop

# Building Spark Docker Image
docker build -f ./spark/Dockerfile . -t cluster:spark

# Building PostgreSQL Docker Image for Hive Metastore Server
docker build -f ./postgresql/Dockerfile . -t cluster:postgresql

# Building Hive Docker Image
docker build -f ./hive/Dockerfile . -t cluster:hive

