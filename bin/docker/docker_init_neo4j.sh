#! /usr/bin/bash

sudo docker run --init --name=neo4j \
  -p 7474:7474 -p 7687:7687 \
  -v neo4j_data:/data \
  -dt \
  neo4j:community
