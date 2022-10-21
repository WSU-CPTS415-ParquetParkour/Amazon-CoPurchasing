#! /usr/bin/python3

import os
import re
import configparser as cfg
from neo4j import GraphDatabase as gdb

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

config = cfg.ConfigParser()
config.read(config_path)

def load(filename):
    # open dataset fiel to read
    data=dict()
    with open(filename, 'r', 1, "utf-8") as dataset:
        for line in dataset:

            #this is a comment line
            if line.startswith('#'):
                continue

            else:

                # when line starts with id, then its a new product
                if line.startswith("Id"):
                    current_id = line.strip().split()[1]
                    data[current_id] = dict();

                # line starts with ASIN
                elif line.startswith("ASIN"):
                    data[current_id]['ASIN'] = line.strip().split()[1]

                # line starts with title
                elif "title" in line:
                    data[current_id]["title"] = line[8:].strip()

                # line starts with group
                elif "group" in line:
                    data[current_id]['group'] = line.strip().split()[1]

                # line starts with salesrank
                elif "salesrank" in line:
                    data[current_id]['salesrank'] = line.strip().split()[1]

                # line starts with simliar
                elif "similar" in line:
                    data[current_id]['similar'] = line.strip().split()[2:]

                elif "discontinued product" in line:
                    data[current_id]['similar'] = "n"
                    data[current_id]["title"] = "n"
                    data[current_id]['group'] = "n"
                    data[current_id]['salesrank'] = "n"

                # line starts with reviews
                #elif "reviews" in line:
                    #data[current_id]['reviews'] = line.strip().split()[1]
    return data

# list of adjacent ASINs
def returnsimilar(data):
    adj = []
    for key in data:
        for item in data[key]['similar']:
            adj.append([int(key), item])
    return adj
    
def rawdatatocsv(rawdata, csvfile):
    with open(csvfile,'w', 1, "utf-8") as csv:
        csv.write('Id,ASIN,title,group,salerank,similar\n')
        for product in rawdata:
            csv.write(str(product[0]) + ','+  str(rawdata[product]['ASIN'])  + ','+  str(rawdata[product]['title']) + ',' +  str(rawdata[product]['group']) + ',' +  str(rawdata[product]['salesrank']) + ',' +  str(rawdata[product]['similar']) + '\n')

def similartocsv(similardata, csvfile):
    with open(csvfile,'w', 1, "utf-8") as csv:
        csv.write('Id,similarASIN\n')
        for product in similardata:
            csv.write(str(product[0]) + ',' + str(product[1]) + '\n')


# Methods for loading data into neo4j db (JR)
class N4J:
    def __init__(self):
        self.endpoint = ''.join(['bolt://', config.get('database_connection', 'dbhost'), ':', config.get('database_connection', 'dbport')])
        self.driver = gdb.driver(
            self.endpoint,
            auth=(config.get('database_connection', 'dbuser'), config.get('database_connection', 'dbpass'))
        )

    def close(self):
        self.driver.close()

    def add_node(self, idx, node_data):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_acp_n4_node, idx, node_data)
            print(result) # Switch to log? (JR)

    @staticmethod
    def _create_acp_n4_node(transaction, idx, node_data):
        # Combine values by unpacking to get interpolation to work without additional string processing - merge operator ('|') only exists for Python 3.9+ (JR)
        # Need to prepend with a character since neo4j does not allow for node names to start with numbers: https://neo4j.com/docs/cypher-manual/current/syntax/naming/ (JR)
        # The node name can easily be replaced with another value, this is just to prototype the process before the data model is ready (JR)
        cypher = 'CREATE (%(idx)s:Product {ASIN:\'%(ASIN)s\', title:\'%(title)s\', group:\'%(group)s\', salesrank:\'%(salesrank)s\'})' % {**{'idx':'a_' + idx}, **node_data}
        result = transaction.run(cypher)
        return


def main():

    #dict of raw data
    # dataset = load("sample.txt")
    # Ensuring that the path is constructed agnostic to the system which runs this (JR)
    dataset = load(os.path.join(project_root, 'data', 'amazon-meta_first25.txt'))

    # Testing sending over the full dataset after extraction instead of inline (may incur additional overhead, need to test) (JR)
    n4_loader = N4J()
    [n4_loader.add_node(x, dataset[x]) for x in dataset]
    n4_loader.close()

    # edges
    adjset = returnsimilar(dataset)
    
    #to csv functions
    #rawdatatocsv(dataset, "rawdata.csv")
    #similartocsv(adjset, "similar.csv")
    print(dataset)
    
   
if __name__ == "__main__":
    main()