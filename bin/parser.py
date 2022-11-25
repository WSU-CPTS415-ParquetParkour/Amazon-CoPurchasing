#! /usr/bin/python3

import os
import re
import json
import configparser as cfg
import time
import string
from datetime import datetime
from collections import Counter
from neo4j import GraphDatabase as gdb

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

config = cfg.ConfigParser()
config.read(config_path)

class PerfMon:
    def __init__(self, caller_name):
        # List of tuples in the form [(timestamp, action)] (JR)
        self.timelog = []
        # Counter() stores values as a dict in the form {'item': n}
        self.counter = Counter()
        self.measured_fn_name = caller_name

    def add_timelog_event(self, action):
        self.timelog += [(time.perf_counter(), action)]

    def increment_counter(self, event):
        self.counter[event] += 1

    def get_all(self):
        return {'timelog': self.timelog, 'event counters': self.counter}

    def summarise(self):
        total_time = self.timelog[-1][0]-self.timelog[0][0]
        total_ops = sum([x for x in self.counter.values()])
        summary = {
            'total duration': f'{total_time:0.4f}',
            'average op time': f'{total_time/len(self.timelog):0.4f}',
            'total ops': f'{total_ops}'
        }
        return summary
    
    def log_all(self):
        datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(project_root, 'var', 'perf')
        summary = self.summarise()

        timelog_path = os.path.join(output_path, '%(datestamp)s_timelog_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})
        counts_path = os.path.join(output_path, '%(datestamp)s_counts_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})
        summary_path = os.path.join(output_path, '%(datestamp)s_summary_%(caller)s.csv' % {'caller':self.measured_fn_name, 'datestamp':datestamp})

        # Ensure that the output path exists (JR)
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        with open(timelog_path, 'w', 1, encoding='utf-8') as log:
            log.write('timestamp,action\n')
            for event in self.timelog:
                log.write('%(ts)s,%(ev)s\n' % ({'ts':str(event[0]), 'ev':str(event[1])}))

        with open(counts_path, 'w', 1, encoding='utf-8') as log:
            log.write('event,n\n')
            for event, count in self.counter.items():
                log.write('%(event)s,%(count)s\n' % ({'event':str(event), 'count':str(count)}))

        with open(summary_path, 'w', 1, encoding='utf-8') as log:
            log.write('measure,value\n')
            for stat, val in summary.items():
                log.write('%(stat)s,%(val)s\n' % ({'stat':str(stat), 'val':str(val)}))


class Parser:
    def __init__(self):
        self.data_repo = os.path.join(project_root, 'data')
        self.products = dict()
        self.categories = dict()
        self.category_map = dict()
        self.reviews = dict()

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
        self.parser_perf = PerfMon('Parser.load')
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

    def export_json_all(self, data=None, dirpath=None):
        if dirpath is None:
            dirpath = self.data_repo
        
        datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Products
        filepath = os.path.join(dirpath, 'products_%(ds)s.json' % {'ds': datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.products, f)
        # Category Map
        filepath = os.path.join(dirpath, 'category_map_%(ds)s.json' % {'ds': datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.category_map, f)
        # Categories
        filepath = os.path.join(dirpath, 'categories_%(ds)s.json' % {'ds': datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.categories, f)
        # Reviews
        filepath = os.path.join(dirpath, 'reviews_%(ds)s.json' % {'ds': datestamp})
        with open(filepath, 'w', 1, 'utf-8') as f:
            json.dump(self.reviews, f)

    def import_json_all(self, timestamp, dirpath=None):
        if dirpath is None:
            dirpath = self.data_repo

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

        datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Product nodes & edges
        filepath_product_node_headers = os.path.join(dirpath, 'n4db_product_node_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_product_node_data = os.path.join(dirpath, 'n4db_product_node_data_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_product_edge_headers = os.path.join(dirpath, 'n4db_product_edge_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_product_edge_data = os.path.join(dirpath, 'n4db_product_edge_data_%(datestamp)s.csv' % {'datestamp': datestamp})

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
        filepath_category_node_headers = os.path.join(dirpath, 'n4db_category_node_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_category_node_data = os.path.join(dirpath, 'n4db_category_node_data_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_category_edge_headers = os.path.join(dirpath, 'n4db_category_edge_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_category_edge_data = os.path.join(dirpath, 'n4db_category_edge_data_%(datestamp)s.csv' % {'datestamp': datestamp})

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
        filepath_review_node_headers = os.path.join(dirpath, 'n4db_review_node_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_review_node_data = os.path.join(dirpath, 'n4db_review_node_data_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_review_edge_headers = os.path.join(dirpath, 'n4db_review_edge_header_%(datestamp)s.csv' % {'datestamp': datestamp})
        filepath_review_edge_data = os.path.join(dirpath, 'n4db_review_edge_data_%(datestamp)s.csv' % {'datestamp': datestamp})

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
            result = session.execute_write(self._create_acp_n4_node, idx, node_data)
            return result # Switch to log? (JR)

    def add_node_set(self, node_dataset):
        # Thin wrapper around add_node() to capture timing data (JR)
        perf = PerfMon('N4J.add_node_set')
        perf.add_timelog_event('init')
        for node in node_dataset:
            self.add_node(node, node_dataset[node])
            perf.add_timelog_event('add node')
            perf.increment_counter('add node')
        perf.add_timelog_event('end')
        perf.log_all()

    def add_edge(self, source, destination, relation_str):
        with self.driver.session() as session:
            result = session.execute_write(self._create_acp_n4_edge, source, destination, relation_str)

    def add_edges(self, edgelist, relation_str):
        perf = PerfMon('N4J.add_edges')
        perf.add_timelog_event('init')
        for pair in edgelist:
            self.add_edge(pair[0], pair[1], relation_str)
            perf.add_timelog_event('add edge')
            perf.increment_counter('add edge')
        perf.add_timelog_event('end')
        perf.log_all()

    def load_csv(self, csv_path):
        perf = PerfMon('N4J.load_csv')
        perf.add_timelog_event('init')
        result = session.execute_write(self._load_acp_csv, csv_path)
        perf.add_timelog_event('end')
        perf.log_all()

    @staticmethod
    def _create_acp_n4_node(transaction, idx, label, node_data):
        # Combine values by unpacking to get interpolation to work without additional string processing - merge operator ('|') only exists for Python 3.9+ (JR)
        # Need to prepend with a character since neo4j does not allow for node names to start with numbers: https://neo4j.com/docs/cypher-manual/current/syntax/naming/ (JR)
        # The node name can easily be replaced with another value, this is just to prototype the process before the data model is ready (JR)
        # cypher = 'CREATE (%(idx)s:Product {ASIN:\'%(ASIN)s\', title:\'%(title)s\', group:\'%(group)s\', salesrank:\'%(salesrank)s\'})' % {**{'idx':'a_' + idx}, **node_data}
        cypher = 'CREATE (Product:\'%(lab)s\' {ASIN:\'%(ASIN)s\', title:\'%(title)s\', group:\'%(group)s\', salesrank:\'%(salesrank)s\'})' % {**{'lab':label}, **node_data}
        result = transaction.run(cypher)
        return
    
    @staticmethod
    def _create_acp_n4_edge(transaction, src, dest, relation):
        # Performance improvement?  Generate edges by all ASINs in 'similar'
        # May require the 'similar' set of ASINs added to each node?  Restructuring of dict?
        # Example:
            # MATCH (a:PRODUCT)
            # WHERE a.ASIN IN ['039474067X','0679730672','0679750541','1400030668','0896086704']
            # RETURN COUNT(a)
        # Mockup:
            # MATCH (a:PRODUCT), (b:PRODUCT)
            # WHERE b.ASIN IN a.similar
            # CREATE (a)-[:%(rel)s]->(b)

        # TODO: Add primary key for nodes; UNIQUE property
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = 'MATCH (a:PRODUCT), (b:PRODUCT) WHERE a.ASIN = \'%(from)s\' AND b.ASIN = \'%(to)s\' CREATE (a)-[:%(rel)s]->(b)' % {'from':src, 'to':dest, 'rel':relation}
        result = transaction.run(cypher)
        return

    @staticmethod
    def _load_acp_csv(transaction, csv):
        cypher = ''
        result = transaction.run(cypher)
        return


def main(mode='parse'):
    datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
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
    main('convert')