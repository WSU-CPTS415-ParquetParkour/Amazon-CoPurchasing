#! /usr/bin/bash

# Retrieve latest Neo4j image
sudo podman pull docker.io/neo4j:community

# Create volume for data storage
sudo podman volume create neo4j_data
