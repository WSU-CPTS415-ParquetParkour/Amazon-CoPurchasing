#! /usr/bin/bash
# If using a separate machine to host the neo4j database, the below commands will help with the import process.

PROJECT_ROOT=$(dirname $(realpath "${BASH_SOURCE[0]}") | sed -e 's/Amazon-CoPurchasing.*/Amazon-CoPurchasing/')
TARGET_HOST="$(crudini --get $PROJECT_ROOT/etc/config.ini database_connection dbhost)"
STAGE_REPO="~/data_store/amazon_copurchasing"

# Rename imported files (optional, makes the db import easier)
# rename -v '_20221120_235612' '' ./data_temp/*

# Import data to container
sudo podman cp "${STAGE_REPO}/." 'neo4j:/var/lib/neo4j/import/'

# Import to database
sudo podman exec neo4j /var/lib/neo4j/bin/neo4j-admin database import full \
    --delimiter="\t" \
    --nodes import/n4db_product_node_header.csv,import/n4db_product_node_data.csv \
    --nodes import/n4db_category_node_header.csv,import/n4db_category_node_data.csv \
    --nodes import/n4db_review_node_header.csv,import/n4db_review_node_data.csv \
    --relationships import/n4db_product_edge_header.csv,import/n4db_product_edge_data.csv \
    --relationships import/n4db_category_edge_header.csv,import/n4db_category_edge_data.csv \
    --relationships import/n4db_review_edge_header.csv,import/n4db_review_edge_data.csv \
    --overwrite-destination=true

# Restart container
sudo podman container restart neo4j
