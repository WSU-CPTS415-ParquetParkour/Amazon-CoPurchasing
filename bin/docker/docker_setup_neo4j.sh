#! /usr/bin/bash

# Retrieve latest Neo4j image
sudo docker pull neo4j:community

# Create volume for data storage
sudo docker volume create neo4j_data
