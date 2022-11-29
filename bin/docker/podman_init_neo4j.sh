#! /usr/bin/bash

sudo podman run --init --name=neo4j \
  -p 7474:7474 -p 7687:7687 \
  -v neo4j_data:/data \
  -dt \
  docker.io/neo4j:community
