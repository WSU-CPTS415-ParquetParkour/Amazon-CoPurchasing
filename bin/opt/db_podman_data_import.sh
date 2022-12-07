#! /usr/bin/bash
# If using a separate machine to host the neo4j database, the below commands will help with the import process.

PROJECT_ROOT=$(dirname $(realpath "${BASH_SOURCE[0]}") | sed -e 's/Amazon-CoPurchasing.*/Amazon-CoPurchasing/')
TARGET_HOST="$(crudini --get $PROJECT_ROOT/etc/config.ini database_connection dbhost)"
STAGE_REPO="~/data_store/amazon_copurchasing"

# Import data to container
sudo podman cp "${STAGE_REPO}/." 'neo4j:/var/lib/neo4j/import/'

# Import to database
sudo podman exec neo4j /var/lib/neo4j/bin/neo4j-admin database import full \
    --delimiter="\t" \
    --nodes import/csv_batches/n4db_product_node_header.csv,import/csv_batches/n4db_product_node_data.csv \
    --nodes import/csv_batches/n4db_category_node_header.csv,import/csv_batches/n4db_category_node_data.csv \
    --nodes import/csv_batches/n4db_review_node_header.csv,import/csv_batches/n4db_review_node_data.csv \
    --nodes import/csv_batches/n4db_customer_node_header.csv,import/csv_batches/n4db_customer_node_data.csv \
    --relationships import/csv_batches/n4db_product_edge_header.csv,import/csv_batches/n4db_product_edge_data.csv \
    --relationships import/csv_batches/n4db_category_edge_header.csv,import/csv_batches/n4db_category_edge_data.csv \
    --relationships import/csv_batches/n4db_review_edge_header.csv,import/csv_batches/n4db_review_edge_data.csv \
    --relationships import/csv_batches/n4db_customer_edge_header.csv,import/csv_batches/n4db_customer_edge_data.csv \
    --id-type=string --skip-bad-relationships --skip-duplicate-nodes=true --overwrite-destination=true

# Alternate database import if data files are split with index IDs appended
# sudo podman exec neo4j /var/lib/neo4j/bin/neo4j-admin database import full \
#     --delimiter="\t" \
#     --nodes import/csv_batches/n4db_product_node_header.csv,'import/csv_batches/n4db_product_node_data_\d+\.csv' \
#     --nodes import/csv_batches/n4db_category_node_header.csv,'import/csv_batches/n4db_category_node_data_\d+\.csv' \
#     --nodes import/csv_batches/n4db_review_node_header.csv,'import/csv_batches/n4db_review_node_data_\d+\.csv' \
#     --nodes import/csv_batches/n4db_customer_node_header.csv,'import/csv_batches/n4db_customer_node_data_\d+\.csv' \
#     --relationships import/csv_batches/n4db_product_edge_header.csv,'import/csv_batches/n4db_product_edge_data_\d+\.csv' \
#     --relationships import/csv_batches/n4db_category_edge_header.csv,'import/csv_batches/n4db_category_edge_data_\d+\.csv' \
#     --relationships import/csv_batches/n4db_review_edge_header.csv,'import/csv_batches/n4db_review_edge_data_\d+\.csv' \
#     --relationships import/csv_batches/n4db_customer_edge_header.csv,'import/csv_batches/n4db_customer_edge_data_\d+\.csv' \
#     --id-type=string --skip-bad-relationships --skip-duplicate-nodes=true --overwrite-destination=true



# Restart container
sudo podman container restart neo4j
