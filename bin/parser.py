#! /usr/bin/python3

import os
import sys
import re
import json
# import logging
import configparser as cfg
from datetime import datetime
from neo4j import GraphDatabase as gdb

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

# Add reference path to access files in /lib/ (JR)
sys.path.insert(0, os.path.join(project_root, 'lib'))

from acpN4J import N4J
from acpPerfMon import PerfMon

config = cfg.ConfigParser()
config.read(config_path)

class Parser:
    def __init__(self, batch_size=500):
        self.data_repo = os.path.join(project_root, 'data')
        self.batch_size = batch_size
        self.products = dict()
        self.categories = dict()
        self.category_map = dict()
        self.reviews = dict()
        self.customer_map = dict() #TODO: GENERATE MAP OF USER IDS SIMILAR TO category_map TO GENERATE USER NODES WHICH POINT TO MULTIPLE REVIEW & PRODUCT NODES
        self.datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.latest_export_timestamp = None

        # logger = logging.getlogger('parser')
        # logger.setLevel(logging.INFO)
        # log_ch = logging.StreamHandler()
        # log_ch.setLevel(logging.INFO)
        # log_formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        # log_ch.setFormatter(log_formatter)
        # logger.addHandler(log_ch)

        self.get_latest_export_timestamp()

    def clean_string(self, string):
        # Escape single quotes which break Python string interpolation (JR)
        # Escape backslashes which break Cypher queries (JR)
        # Correct misordered quotes & commas (JR)
        # Correct excess spacing around colons (JR)
        return string.strip().replace('\\', '\\\\').replace("'", "\\'").replace(",\"", "\",").replace('"', '\\\"').replace('\t', '').replace('  ', ' ').replace(' :', ':')

    def load(self, filename):
        self.parser_perf = PerfMon('Parser.load')
        self.parser_perf.add_timelog_event('init')
        # open dataset field to read
        data=dict()
        with open(filename, 'r', 1, "utf-8") as dataset:
            for line in dataset:

                current_line = line.strip()

                #this is a comment or empty line
                if current_line.startswith('#') or current_line == '':
                    continue

                else:
                    # Collect the major property key from the current line (JR)
                    property_key = re.findall('^(.+)(?=:)', current_line) if current_line != '' else ''
                    # when line starts with id, then its a new product
                    # if line.startswith("Id"):
                    if 'Id' in property_key:
                        current_id = re.findall('(?<=%(prop)s:)\s+(.+)$' % {'prop': property_key}, current_line)[0]
                        data[current_id] = dict()

                    # line starts with ASIN
                    # elif line.strip().startswith("ASIN"):
                    elif 'ASIN' in property_key:
                        data[current_id]['ASIN'] = line.strip().split(': ')[1]

                    # line starts with title
                    # elif line.strip().startswith("title:"):
                    elif 'title' in property_key:
                        data[current_id]["title"] = self.clean_string(line[8:])

                    # line starts with group
                    # elif line.strip().startswith("group:"):
                    elif 'group' in property_key:
                        data[current_id]['group'] = line.strip().split(': ')[1]

                    # line starts with salesrank
                    # elif line.strip().startswith("salesrank"):
                    elif 'salesrank' in property_key:
                        data[current_id]['salesrank'] = line.strip().split(': ')[1]

                    # line starts with simliar
                    # elif line.strip().startswith("similar"):
                    elif 'similar' in property_key:
                        data[current_id]['similar_to'] = ';'.join(line.strip().split()[2:])

                    elif line.strip().startswith("discontinued product"):
                        data[current_id]["title"] = ''
                        data[current_id]['group'] = ''
                        data[current_id]['salesrank'] = ''
                        data[current_id]['similar_to'] = ''
                        # data[current_id]['similar_n'] = ''
                        # data[current_id]['similar_n_match'] = ''

                    # line starts with reviews
                    #elif "reviews" in line:
                        #data[current_id]['reviews'] = line.strip().split()[1]

                # Update performance counters (JR)
                self.parser_perf.add_timelog_event('parse line')
                self.parser_perf.increment_counter('parse line')

            self.parser_perf.add_timelog_event('end')
            self.parser_perf.log_all()

        return data

    # Alternate version of load() testing out match-case
    def load_switched(self, filename):
        self.parser_perf = PerfMon('Parser.load_switched')
        self.parser_perf.add_timelog_event('init')

        review_idx = 0
        category_idx = 0

        with open(filename, 'r', 1, "utf-8") as dataset:
            for line in dataset:

                # Reducing function call counts since this was a common operation (JR)
                current_line = line.strip()
                review_date = re.findall('^\d{4}-\d{1,2}-\d{1,2}', current_line)

                #this is a comment or empty line
                if current_line.startswith('#') or current_line == '':
                    continue

                elif current_line.startswith("discontinued product"):
                    self.products[current_id]["title"] = ''
                    self.products[current_id]['group'] = ''
                    self.products[current_id]['salesrank'] = ''
                    self.products[current_id]['similar_to'] = ''
                    self.products[current_id]['category_ct'] = ''
                    self.products[current_id]['review_ct'] = ''
                    self.products[current_id]['review_downloaded'] = ''
                    self.products[current_id]['review_avg_rating'] = ''
                    # self.products[current_id]['similar_n'] = ''
                    # self.products[current_id]['similar_n_match'] = ''

                elif current_line.startswith('|'):
                    current_category_id = 'cat%(c)s' % {'c': category_idx}
                    if current_id not in self.categories.keys():
                        # category_tmp = list()
                        self.categories[current_id] = dict()

                    # Build map of unique paths, expanding only when a new path is detected
                    if current_line not in self.category_map.keys():
                        self.category_map[current_line] = current_category_id
                        category_idx += 1

                    if current_category_id not in self.categories[current_id].keys():
                        self.categories[current_id][self.category_map[current_line]] = dict()
                    
                    # This could probably be restructured as a flat list, but maintaining nested dictionary pattern for consistency (JR)
                    self.categories[current_id][self.category_map[current_line]] = current_line

                elif len(review_date) > 0:
                    current_review_id = 'rev%(r)s' % {'r': review_idx}
                    if current_id not in self.reviews.keys():
                        self.reviews[current_id] = dict()
                    
                    if current_review_id not in self.reviews[current_id].keys():
                        self.reviews[current_id][current_review_id] = dict()

                    # Unwind dictionaries and join them together (JR)
                    # 'customer' is misspelled as 'cutomer' (missing 's') in the data (JR)
                    self.reviews[current_id][current_review_id] = {
                        **{'review_date':review_date[0]},
                        **{x:y for x,y in re.findall('(\w+):\s+(\w+|\d+)', current_line.replace('cutomer', 'customer'))}
                    }
                    review_idx += 1

                # By the ordering of the data in amazon-meta.txt this will be hit first,
                # allowing property_key to be available in the prior conditions (JR)
                else:
                    # Collect the major property key from the current line (JR)
                    property_key = re.findall('^(\w+)(?=:)', current_line)
                    # Jedi 0.15.12 seems to not understand the match-case syntax, but Python 3.10+ executes as expected (JR)
                    match property_key:
                        case ['Id']:
                            current_id = re.findall('(?<=%(prop)s:)\s+(.+)$' % {'prop': property_key}, current_line)[0]
                            self.products[current_id] = dict()
                        case ['ASIN']:
                            self.products[current_id]['ASIN'] = current_line.split(': ')[1]
                        case ['title']:
                            self.products[current_id]["title"] = self.clean_string(line[8:])
                        case ['group']:
                            self.products[current_id]['group'] = current_line.split(': ')[1]
                        case ['salesrank']:
                            self.products[current_id]['salesrank'] = current_line.split(': ')[1]
                        case ['similar']:
                            self.products[current_id]['similar_to'] = ';'.join(current_line.split()[2:])
                        case ['categories']:
                            # Collected in the default section below
                            self.products[current_id]['category_ct'] = re.findall('\d+', current_line.replace('  ', ' '))[0]
                        case ['reviews']:
                            review_meta = {x:y for x,y in re.findall('(?<=\s)(\w+\s*\w*):\s+(\d+)', current_line.replace('  ',' '))}
                            self.products[current_id] = {
                                **self.products[current_id],
                                **{"review_%(y)s" % {'y': x.replace(' ', '_')}:review_meta[x] for x in review_meta}
                            }
                        case _:
                            # Ignore the line by default - if it's important, it needs to be allocated above (JR)
                            continue

                # Update performance counters (JR)
                self.parser_perf.add_timelog_event('parse line')
                self.parser_perf.increment_counter('parse line')

            self.parser_perf.add_timelog_event('end')
            self.parser_perf.log_all()

        return

    # list of adjacent ASINs
    def returnsimilar(self, data):
        adj = []
        # Close, but needs one less layer of nesting; going to swap values directly in the nested for loop for now (JR)
        # [[[data[y]['ASIN'], x] for x in data[y]['similar']] for y in data]
        for key in data:
            for item in data[key]['similar']:
                adj.append([data[key]['ASIN'], item])
        return adj

    def rawdatatocsv(self, rawdata, csvfile):
        with open(csvfile,'w', 1, "utf-8") as csv:
            csv.write('Id,ASIN,title,group,salerank,similar\n')
            for product in rawdata:
                csv.write(str(product[0]) + ','+  str(rawdata[product]['ASIN'])  + ','+  str(rawdata[product]['title']) + ',' +  str(rawdata[product]['group']) + ',' +  str(rawdata[product]['salesrank']) + ',' +  str(rawdata[product]['similar']) + '\n')

    def similartocsv(self, similardata, csvfile):
        with open(csvfile,'w', 1, "utf-8") as csv:
            csv.write('Id,similarASIN\n')
            for product in similardata:
                csv.write(str(product[0]) + ',' + str(product[1]) + '\n')

    def similar_asin_to_id(self):
        # translates ASIN values to ids generated during parsing, adds new property to product metadata
        base_map = {self.products[x]['ASIN']:x for x in self.products}
        for product in self.products:
            sim_asins = self.products[product]['similar_to']
            if sim_asins != '':
                self.products[product]['similar_to_ids'] = ';'.join([base_map[x] for x in sim_asins.split(';') if x in base_map.keys()])

    def get_latest_export_timestamp(self):
        export_history_path = os.path.join(project_root, 'var', 'logs')
        export_history_log = os.path.join(export_history_path, 'export_history.log')
        latest_timestamp = None
        if not os.path.exists(export_history_path):
            os.makedirs(export_history_path)
        
        if not os.path.isfile(export_history_log):
            open(export_history_log, 'a').close()
        
        else:
            # Open the history file in binary mode to read backwards and get the latest entry (JR)
            with open(export_history_log, 'rb') as h:
                try:
                    h.seek(-2, os.SEEK_END)
                    while h.read(1) != b'\n':
                        h.seek(-2, os.SEEK_CUR)
                except OSError: # In case there is only one line in the file (JR)
                    h.seek(0)
                latest_timestamp = h.readline().decode().strip()

        return latest_timestamp

    def add_export_timestamp(self, timestamp):
        export_history_path = os.path.join(project_root, 'var', 'logs')
        export_history_log = os.path.join(export_history_path, 'export_history.log')
        latest_timestamp = None
        if not os.path.exists(export_history_path):
            os.makedirs(export_history_path)
        
        if not os.path.isfile(export_history_log):
            open(export_history_log, 'a').close()
        
        else:
            # Open the history file in binary mode to read backwards and get the latest entry (JR)
            with open(export_history_log, 'a') as h:
                h.write(timestamp + '\n')
        return

    def export_json_all(self, data=None, dirpath=None):
        if dirpath is None:
            dirpath = self.data_repo
        
        self.add_export_timestamp(self.datestamp)

        # Products
        filepath = os.path.join(dirpath, 'products_%(ds)s.json' % {'ds': self.datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.products, f)
        # Category Map
        filepath = os.path.join(dirpath, 'category_map_%(ds)s.json' % {'ds': self.datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.category_map, f)
        # Categories
        filepath = os.path.join(dirpath, 'categories_%(ds)s.json' % {'ds': self.datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.categories, f)
        # Reviews
        filepath = os.path.join(dirpath, 'reviews_%(ds)s.json' % {'ds': self.datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.reviews, f)

    def import_json_all(self, timestamp, dirpath=None):
        if dirpath is None:
            dirpath = self.data_repo

        if timestamp is None:
            if self.latest_export_timestamp is None:
                raise Exception('No prior timestamps logged.')
            else:
                timestamp = self.latest_export_timestamp

        filepath = os.path.join(dirpath, 'products_%(ts)s.json' % {'ts': timestamp})

        with open(filepath, 'r') as j:
            self.products = json.load(j)
        filepath = os.path.join(dirpath, 'category_map_%(ts)s.json' % {'ts': timestamp})
        with open(filepath, 'r') as j:
            self.category_map = json.load(j)
        filepath = os.path.join(dirpath, 'categories_%(ts)s.json' % {'ts': timestamp})
        with open(filepath, 'r') as j:
            self.categories = json.load(j)
        filepath = os.path.join(dirpath, 'reviews_%(ts)s.json' % {'ts': timestamp})
        with open(filepath, 'r') as j:
            self.reviews = json.load(j)
        return

    def export_neo4j_db_csv(self, data=None, dirpath=None):
        if dirpath is None:
            dirpath = self.data_repo

        # Product nodes & edges
        filepath_product_node_headers = os.path.join(dirpath, 'n4db_product_node_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_product_node_data = os.path.join(dirpath, 'n4db_product_node_data_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_product_edge_headers = os.path.join(dirpath, 'n4db_product_edge_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_product_edge_data = os.path.join(dirpath, 'n4db_product_edge_data_%(ds)s.csv' % {'ds': self.datestamp})

        with open(filepath_product_node_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join(['Id:ID', 'ASIN:string', 'title:string', 'group:string', 'salesrank:long', 'category_ct:int', 'review_ct:int', 'review_downloaded:int', 'review_avg_rating:int', ':LABEL']))

        with open(filepath_product_edge_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join([':START_ID', ':END_ID', ':TYPE']))

        with open(filepath_product_node_data, 'w', 1, 'utf-8') as csv:
            for product in self.products:
                output = '\t'.join([product, '\t'.join([str(x[1]).strip() for x in self.products[product].items() if not x[0].startswith('similar')]), 'PRODUCT'])
                csv.write(output + '\n')

        with open(filepath_product_edge_data, 'w', 1, 'utf-8') as csv:
            for product in self.products:
                if 'similar_to_ids' in self.products[product].keys():
                    for sim_id in self.products[product]['similar_to_ids'].split(';'):
                        if sim_id != '' and sim_id is not None:
                            csv.write('\t'.join([product, sim_id, 'IS_SIMILAR_TO\n']))
        
        # Category nodes & edges
        filepath_category_node_headers = os.path.join(dirpath, 'n4db_category_node_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_category_node_data = os.path.join(dirpath, 'n4db_category_node_data_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_category_edge_headers = os.path.join(dirpath, 'n4db_category_edge_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_category_edge_data = os.path.join(dirpath, 'n4db_category_edge_data_%(ds)s.csv' % {'ds': self.datestamp})

        with open(filepath_category_node_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join(['Id:ID', 'category_path:string', ':LABEL']))

        with open(filepath_category_edge_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join([':START_ID', ':END_ID', ':TYPE']))

        with open(filepath_category_node_data, 'w', 1, 'utf-8') as csv:
            # for product_id in self.categories:
                # for cat_id in self.categories[product_id]:
            for category in self.category_map:
                output = '\t'.join([self.category_map[category], category, 'CATEGORY'])
                csv.write(output + '\n')

        with open(filepath_category_edge_data, 'w', 1, 'utf-8') as csv:
            for product_id in self.categories:
                for cat_id in self.categories[product_id].keys():
                    csv.write('\t'.join([product_id, cat_id, 'CATEGORIZED_AS\n']))

        # Review nodes & edges
        filepath_review_node_headers = os.path.join(dirpath, 'n4db_review_node_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_review_node_data = os.path.join(dirpath, 'n4db_review_node_data_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_review_edge_headers = os.path.join(dirpath, 'n4db_review_edge_header_%(ds)s.csv' % {'ds': self.datestamp})
        filepath_review_edge_data = os.path.join(dirpath, 'n4db_review_edge_data_%(ds)s.csv' % {'ds': self.datestamp})

        with open(filepath_review_node_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join(['Id:ID', 'review_date:date', 'customer:string', 'rating:int', 'votes:int', 'helpful:int', ':LABEL']))

        with open(filepath_review_edge_headers, 'w', 1, 'utf-8') as csv:
            csv.write('\t'.join([':START_ID', ':END_ID', ':TYPE']))

        with open(filepath_review_node_data, 'w', 1, 'utf-8') as csv:
            for product_id in self.reviews:
                for rev_id in self.reviews[product_id]:
                    output = '\t'.join([rev_id, '\t'.join([str(x).strip() for x in self.reviews[product_id][rev_id].values()]), 'REVIEW'])
                    csv.write(output + '\n')

        with open(filepath_review_edge_data, 'w', 1, 'utf-8') as csv:
            for product_id in self.reviews:
                for rev_id in self.reviews[product_id].keys():
                    csv.write('\t'.join([product_id, rev_id, 'REVIEWED_BY\n']))


def main(mode='parse'):
    parser = Parser()

    if mode == 'parse':
        print('parsing data from amazon-meta.txt')
        #dict of raw data
        # Ensuring that the path is constructed agnostic to the system which runs this (JR)
        # dataset = parser.load(os.path.join(project_root, 'data', 'amazon-meta.txt'))
        # testing out match-case
        parser.load_switched(os.path.join(project_root, 'data', 'amazon-meta.txt'))
        print('creating ASIN map for similar products')
        parser.similar_asin_to_id()

        print('exporting restructured data as json')
        parser.export_json_all()

        print('exporting as Neo4j db components for "neo4j-admin import database"')
        parser.export_neo4j_db_csv()

    elif mode == 'convert':
        # Importing prior export from JSON (JR)
        print('importing prior export')
        parser.import_json_all(timestamp='20221121_163028')

        # No longer needed at this point (normally) since these are created during the initial parse workflow above (JR)
        # print('converting ASINs to node ids')
        # parser.similar_asin_to_id()

        print('exporting as Neo4j db components for "neo4j-admin import database"')
        parser.export_neo4j_db_csv()

    elif mode == 'upload':
        # Testing sending over the full dataset after extraction instead of inline (may incur additional overhead, need to test) (JR)
        # Moved dict iteration into separate function in N4J to capture performance stats
        n4_loader = N4J()
        # n4_loader.add_node_set(dataset)

        # edges
        adjset = parser.returnsimilar(dataset)

        n4_loader.add_edges(adjset, 'IS_SIMILAR_TO')
        n4_loader.close()

        #to csv functions
        #rawdatatocsv(dataset, "rawdata.csv")
        #similartocsv(adjset, "similar.csv")
        # print(dataset)
    
   
if __name__ == "__main__":
    main('parse')
