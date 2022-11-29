#! /usr/bin/sh

PROJECT_ROOT=$(dirname $(realpath "${BASH_SOURCE[0]}") | sed -e 's/Amazon-CoPurchasing.*/Amazon-CoPurchasing/')
TARGET_HOST="$(crudini --get $PROJECT_ROOT/etc/config.ini database_connection dbhost)"
STAGE_REPO="~/data_store/amazon_copurchasing/"

# Upload data to server machine
rsync -ravu --rsh=ssh --filter='+ n4db_*.csv' --filter='- *' "${PROJECT_ROOT}/data/" "${TARGET_HOST}:${STAGE_REPO}"
