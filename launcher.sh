#! /usr/bin/bash

# """
# Title: Amazon Co-Purchasing
# Team: Parket Parkour
# Contributors: Catherine Dennis, Alex King, Andrew Lemly, Sean McCord, Jarret Russell
# GitHub Repo: https://github.com/WSU-CPTS415-ParquetParkour/Amazon-CoPurchasing
# Version: 1.0
# Date: 2002-12-10
#
# Description:    Tools to parse SNAP dataset, setup database containers, deploy data.
#                 Application GUI which provides a SQL-like interface for queries
#                 against a neo4j graph database and perform collaborative filtering
#                 on the resulting subset.
# """

PROJECT_ROOT=$(dirname $(realpath "${BASH_SOURCE[0]}") | sed -e 's/Amazon-CoPurchasing.*/Amazon-CoPurchasing/')

python3 ${PROJECT_ROOT}/bin/app.py
