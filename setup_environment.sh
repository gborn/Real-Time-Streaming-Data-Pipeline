#!/bin/bash

# install docker
sudo apt-get remove docker docker-engine docker.io containerd runc
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io

# enable docker
sudo systemctl enable docker.service
sudo systemctl enable containerd.service

# create docker subnet
sudo docker network create --subnet=172.20.0.0/16 my_subnet

# start zookeeper and kafka containers
sudo docker pull zookeeper
sudo docker pull ches/kafka

# before running, verify IP using ifconfig(install net-tools if needed)
HOST_IP=$(ip -o route get to 8.8.8.8 | sed -n 's/.*src \([0-9.]\+\).*/\1/p')
export HOST_IP_ADDRESS=$HOST_IP

sudo docker run -d --hostname zookeepernode --net my_subnet --ip 172.20.1.3 --name my_zookeeper --publish 2181:2181 zookeeper:latest

sudo docker run -d --hostname kafkanode --net my_subnet --ip 172.20.1.4 --name my_kafka --publish 9092:9092 --publish 7203:7203 --env KAFKA_ADVERTISED_HOST_NAME=${HOST_IP} --env ZOOKEEPER_IP=${HOST_IP} ches/kafka

# install hadoop
# steps to install Git and download project

sudo bash hadoop_spark_cluster/1_create_images.sh
sudo bash hadoop_spark_cluster/2_create_cluster.sh create

# install python and setup jupyter notebook server
sudo ln -s /usr/bin/python3 /usr/bin/python

sudo apt update
sudo apt install python3-pip
sudo apt install python3-notebook jupyter jupyter-core

pip install jupyter_http_over_ws
jupyter serverextension enable --py jupyter_http_over_ws