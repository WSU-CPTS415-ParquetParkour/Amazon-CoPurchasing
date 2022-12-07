#! /usr/bin/python3

import os
import sys
import re
import json
# import logging
import configparser as cfg
import numpy as np
import hashlib as hl
from datetime import datetime
from multiprocessing import Pool, Process
from joblib import Parallel, delayed, cpu_count
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
    def __init__(self, batch_size=1000):
        self.data_repo = os.path.join(project_root, 'data')
        self.batch_size = batch_size
        self.products = dict()
        self.categories = dict()
        self.category_map = dict()
        self.reviews = dict()
        self.customer_history = dict()
        self.customers = dict()
        self.summaries = {'product': dict(), 'category': dict(), 'review': dict(), 'customer': dict()}
        self.datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.latest_export_timestamp = None
        self.export_vars = ['product', 'category', 'review', 'customer']
        self.precision = 3

        # logger = logging.getlogger('parser')
        # logger.setLevel(logging.INFO)
        # log_ch = logging.StreamHandler()
        # log_ch.setLevel(logging.INFO)
        # log_formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        # log_ch.setFormatter(log_formatter)
        # logger.addHandler(log_ch)

        self.get_latest_export_timestamp()

    def clear_datasets(self, dataset=None):
        if dataset is None:
            self.products = dict()
            self.categories = dict()
            self.category_map = dict()
            self.reviews = dict()
            self.customer_history = dict()
            self.customers = dict()
        
        else:
            match dataset:
                case 'product':
                    self.products = dict()
                case 'category':
                    self.categories = dict()
                    self.category_map = dict()
                case 'review':
                    self.reviews = dict()
                case 'customer':
                    self.customers = dict()
                    self.customer_history = dict()

    def clean_string(self, string):
        # Escape single quotes which break Python string interpolation (JR)
        # Escape backslashes which break Cypher queries (JR)
        # Correct misordered quotes & commas (JR)
        # Correct excess spacing around colons (JR)
        return string.strip().replace('\\', '\\\\').replace("'", "\\'").replace(",\"", "\",").replace('"', '\\\"').replace('\t', '').replace('  ', ' ').replace(' :', ':')

    def split_file(self, filename):
        current_id = None
        review_idx = 0
        category_idx = 0
        batch_items = 0
        batch_idx = 0
        item_data = list()

        node_fields = {
            'product':          ['ASIN', 'title', 'group', 'salesrank', 'similar_to', 'category_path_ct', 'category_path_depth_avg', 'category_path_depth_sd', 'review_ct', 'review_downloaded_ct', 'review_rating_avg', 'review_rating_sd', 'review_votes_total', 'review_votes_avg', 'review_votes_sd', 'review_mttr', 'review_helpful_ratio_avg', 'review_helpful_ratio_sd', 'review_rating_avg_wtd', 'review_rating_wtd_avg', 'review_rating_wtd_sd'],
            'review':           ['Id', 'customer', 'helpful', 'rating', 'review_date', 'votes', 'review_helpful_ratio', 'review_rating_wtd'],
            'category':         ['Id', 'category_path', 'path_depth'],
            'customer':         ['Id', 'review_ct', 'review_mttr', 'helpful_avg', 'helpful_sd', 'rating_avg', 'rating_sd', 'rating_avg_wtd', 'votes_total', 'votes_avg', 'votes_sd', 'helpful_ratio_avg', 'helpful_ratio_sd'],
            'summary_product':  ['path_depth_avg', 'path_depth_sd'],
            'summary_review':   ['product_ct'],
            'summary_category': ['path_depth_avg', 'path_depth_sd'],
            'summary_customer': ['customer_unique_ct']
        }

        with open(filename, 'r', 1, "utf-8") as dataset:
            for line in dataset:

                current_line = self.clean_string(line)
                review_date = re.findall('^\d{4}-\d{1,2}-\d{1,2}', current_line)

                if current_line.startswith('#') or current_line.startswith("discontinued product"):
                    continue
                elif current_line == '' and current_id in self.products.keys():
                    batch_items += 1
                    if batch_items >= self.batch_size:
                        with open(os.path.join(self.data_repo, 'split_data', ''.join([str(batch_idx).zfill(6), '.txt'])), 'w', 1, 'utf-8') as f:
                            for row in item_data:
                                f.write(row + '\n')
                            f.write('\n')
                            batch_idx += 1
                        item_data = list()
                        batch_items = 0
                    else:
                        item_data.append('\n')

                else:
                    # Collect the major property key from the current line (JR)
                    property_key = re.findall('^(\w+)(?=:)', current_line)
                    # Jedi 0.15.12 seems to not understand the match-case syntax, but Python 3.10+ executes as expected (JR)
                    match property_key:
                        case ['Id']:
                            current_id = re.findall('(?<=%(prop)s:)\s+(.+)$' % {'prop': property_key}, current_line)[0]
                            # Initialize all fields as empty strings by default (JR)
                            self.products[current_id] = {x:'' for x in node_fields['product']}
                    if current_id is not None:
                        item_data.append(self.clean_string(line))

            # Record any lingering data (JR)
            if batch_items > 0:
                with open(os.path.join(self.data_repo, 'split_data', ''.join([str(batch_idx).zfill(6), '.txt'])), 'w', 1, 'utf-8') as f:
                    for row in item_data:
                        f.write(row + '\n')
                    f.write('\n')


    # Alternate version of load_batched() for execution in parallel
    def load_split(self, filename):
        self.parser_perf = PerfMon('Parser.load_split')
        self.parser_perf.add_timelog_event('init')

        node_fields = {
            'product':          ['ASIN', 'title', 'group', 'salesrank', 'similar_to', 'similar_to_ct', 'category_path_ct', 'category_path_depth_avg', 'category_path_depth_sd', 'review_total_ct', 'review_downloaded_ct', 'review_rating_avg', 'review_rating_sd', 'review_votes_total', 'review_votes_avg', 'review_votes_sd', 'review_mttr', 'review_helpful_ratio_avg', 'review_helpful_ratio_sd', 'review_rating_avg_wtd', 'review_rating_wtd_avg', 'review_rating_wtd_sd', 'customers_unique'],
            'review':           ['Id', 'customer', 'helpful', 'rating', 'review_date', 'votes', 'review_helpful_ratio', 'review_rating_wtd'],
            'category':         ['path', 'path_depth'],
            'customer':         ['Id', 'review_ct', 'helpful_avg', 'helpful_sd', 'rating_avg', 'rating_sd', 'rating_avg_wtd', 'votes_total', 'votes_avg', 'votes_sd', 'helpful_ratio_avg', 'helpful_ratio_sd'],
            'summary_product':  ['path_depth_avg', 'path_depth_sd'],
            'summary_review':   ['product_ct'],
            'summary_category': ['path_depth_avg', 'path_depth_sd'],
            'summary_customer': ['customer_unique_ct']
        }

        current_id = None
        # review_idx = 0
        # category_idx = 0
        batch_idx = 0
        file_segment = False

        input_file_idx = re.findall('\d{6}(?=\.txt)', filename)

        if len(input_file_idx) != 0:
            file_segment = True
            batch_idx = int(input_file_idx[0])

        with open(filename, 'r', 1, "utf-8") as dataset:
            for line in dataset:

                # Reducing function call counts since this was a common operation (JR)
                current_line = line.strip()
                review_date = re.findall('^\d{4}-\d{1,2}-\d{1,2}', current_line)

                #this is a comment or empty line
                if current_line.startswith('#') or (current_line == '' and current_id is None):
                    continue

                # This will not get hit when using the split dataset as these lines are excluded (JR)
                elif current_line.startswith("discontinued product"):
                    #TODO: Validate if this is necessary now that all fields are initialized when an ID is found
                    self.products[current_id] = {x:None for x in node_fields['product']}

                elif current_line.startswith('|'):
                    # current_category_id = 'cat%(c)s' % {'c': category_idx}
                    current_category_id = hl.md5(current_line.encode('utf-8')).hexdigest()
                    if current_id not in self.categories.keys():
                        self.categories[current_id] = dict()

                    # Build map of unique paths, expanding only when a new path is detected
                    if current_line not in self.category_map.keys():
                        self.category_map[current_line] = current_category_id
                        # category_idx += 1

                    if current_category_id not in self.categories[current_id].keys():
                        self.categories[current_id][self.category_map[current_line]] = dict()
                    
                    # This could probably be restructured as a flat list, but maintaining nested dictionary pattern for consistency (JR)
                    self.categories[current_id][self.category_map[current_line]]['path'] = current_line
                    self.categories[current_id][self.category_map[current_line]]['path_depth'] = len(re.findall('\|', current_line))
                    

                elif len(review_date) > 0:
                    #TODO: NEED TO COMPILE UNIQUE CHECKSUM FOR REVIEW LINE TO USE AS ID ACROSS THREADS
                    # current_review_id = 'rev%(r)s' % {'r': review_idx}
                    current_review_id = hl.md5(' '.join([current_id, current_line]).encode('utf-8')).hexdigest()
                    if current_id not in self.reviews.keys():
                        self.reviews[current_id] = dict()
                    
                    if current_review_id not in self.reviews[current_id].keys():
                        self.reviews[current_id][current_review_id] = {x:'' for x in node_fields['review']}

                    # Unwind dictionaries and join them together (JR)
                    # 'customer' is misspelled as 'cutomer' (missing 's') in the data (JR)
                    current_review = {x:y for x,y in re.findall('(\w+):\s+(\w+|\d+)', current_line.replace('cutomer', 'customer'))}
                    helpful_ratio = 0 if current_review['votes'] == '0' else round(float(current_review['helpful'])/float(current_review['votes']), self.precision)
                    self.reviews[current_id][current_review_id] = {
                        **{'review_date':review_date[0]},
                        **current_review,
                        **{
                            'review_helpful_ratio': helpful_ratio,
                            'review_rating_wtd': 0 if helpful_ratio == 0 else round((float(current_review['rating']) * float(helpful_ratio))/float(helpful_ratio), self.precision)
                        }
                    }

                    if current_review['customer'] not in self.customer_history.keys():
                        self.customer_history[current_review['customer']] = dict()

                    if current_review_id not in self.customer_history[current_review['customer']].keys():
                        self.customer_history[current_review['customer']][current_review_id] = dict()

                    self.customer_history[current_review['customer']][current_review_id] = {
                        # Should the current ASIN/product id be included here?  Would be more direct, but is ultimately redundant since the review id is embedded. (JR)
                        **{'review_date':review_date[0]},
                        **{x:y for x,y in current_review.items() if x != 'customer'},
                        **{'helpful_ratio': helpful_ratio}
                    }

                    # review_idx += 1
                
                elif current_id is not None and current_line == '':
                    if len(self.products) >= self.batch_size or len(self.categories) >= self.batch_size or len(self.reviews) >= self.batch_size:
                        # dump data to disk
                        # self.dump_neo4j_db_csvs(batch_id=str(batch_idx).zfill(6))

                        # testing batch output as json
                        self.dump_json(batch_id=str(batch_idx).zfill(6))
                        if not file_segment:
                            batch_idx += 1

                    # Perform summary calculations for the current ID now that all data is collected (JR)
                    # Embedding summary statistics into product node only if reviews have been documented for the current product (JR)
                    if current_id in self.reviews.keys():
                        # Collect and preprocess values (JR)
                        ratings_values          = [float(y['rating']) for x,y in self.reviews[current_id].items()]
                        ratings_wtd_values      = [float(y['review_rating_wtd']) for x,y in self.reviews[current_id].items()]
                        votes_values            = [int(y['votes']) for x,y in self.reviews[current_id].items()]
                        review_dates            = [datetime.strptime(y['review_date'], '%Y-%m-%d') for x,y in self.reviews[current_id].items()]
                        days_between_reviews    = [x.days for x in np.ediff1d([d for d in sorted(review_dates)])]
                        hr_values               = [float(y['review_helpful_ratio']) for x,y in self.reviews[current_id].items()]
                        hr_avg                  = np.mean(hr_values)
                        customer_ids            = set(y['customer'] for x,y in self.reviews[current_id].items())
                        rating_avg_wtd          = 0 if hr_avg == 0.0 else (np.mean(ratings_values) * hr_avg)/float(hr_avg)

                        # Apply aggregate calculations (JR)
                        self.products[current_id] = {
                            **self.products[current_id],
                            **{
                                'review_rating_avg'         : round(np.mean(ratings_values), self.precision),
                                'review_rating_sd'          : round(np.std(ratings_values), self.precision),
                                'review_votes_total'        : int(np.sum(votes_values)),
                                'review_votes_avg'          : round(np.mean(votes_values), self.precision),
                                'review_votes_sd'           : round(np.std(votes_values), self.precision),
                                'review_mttr'               : 0 if len(days_between_reviews) == 0 else round(np.mean(days_between_reviews), self.precision),
                                'review_helpful_ratio_avg'  : round(hr_avg, self.precision),
                                'review_helpful_ratio_sd'   : round(np.std(hr_values), self.precision),
                                'review_rating_avg_wtd'     : round(rating_avg_wtd, self.precision),
                                'review_rating_wtd_avg'     : round(np.mean(ratings_wtd_values), self.precision),
                                'review_rating_wtd_sd'      : round(np.std(ratings_wtd_values), self.precision),
                                'customers_unique'          : len(customer_ids)
                            }
                        }

                        # self.reviews[current_id] = {
                        #     **self.reviews[current_id],
                        #     **{
                                
                        #     }
                        # }
                    
                    if current_id in self.categories.keys():
                        # Collect and preprocess values (JR)
                        # path_lengths = [float(len(re.findall('\|', self.categories[current_id][c]))) for c in self.categories[current_id]]
                        path_depths = [float(self.categories[current_id][c]['path_depth']) for c in self.categories[current_id]]
                        # Apply aggregate calculations (JR)
                        self.products[current_id]['category_path_ct']           = len(path_depths)
                        self.products[current_id]['category_path_depth_avg']    = round(np.mean(path_depths), self.precision)
                        self.products[current_id]['category_path_depth_sd']     = round(np.std(path_depths), self.precision)

                # By the ordering of the data in amazon-meta.txt this will be hit first,
                # allowing property_key to be available in the prior conditions (JR)
                else:
                    # Collect the major property key from the current line (JR)
                    property_key = re.findall('^(\w+)(?=:)', current_line)[0]
                    # Jedi 0.15.12 seems to not understand the match-case syntax, but Python 3.10+ executes as expected (JR)
                    match property_key:
                        case 'Id':
                            current_id = re.findall('(?<=%(prop)s:)\s+(.+)$' % {'prop': property_key}, current_line)[0]
                            # Initialize all fields as empty strings by default (JR)
                            self.products[current_id] = {x:'' for x in node_fields['product']}
                        case ('ASIN' | 'title' | 'group'):
                            self.products[current_id][property_key] = current_line.split(': ')[1]
                        case 'salesrank':
                            self.products[current_id][property_key] = int(current_line.split(': ')[1])
                        case 'similar':
                            current_similar = current_line.split()[2:]
                            self.products[current_id]['similar_to'] = ';'.join(current_similar)
                            self.products[current_id]['similar_to_ct'] = len(current_similar)
                        case 'categories':
                            # Collected in the default section below
                            self.products[current_id]['category_path_ct'] = re.findall('\d+', current_line.replace('  ', ' '))[0]
                        case 'reviews':
                            # Only pulling the total and download count here. (JR)
                            # Average rating will be calculated with other summary statistics by aggregation of values across each review entry (JR)
                            # 'review_ct', 'review_downloaded_ct', 'review_rating_avg'
                            review_meta = {x:y for x,y in re.findall('(?<=\s)(\w+\s*\w*):\s+(\d+)', current_line.replace('  ',' ')) if x != 'avg rating'}
                            self.products[current_id] = {
                                **self.products[current_id],
                                **{"review_%(z)s_ct" % {'z': x}:int(y) for x,y in review_meta.items()}
                            }
                        case _:
                            # Ignore the line by default - if it's important, it needs to be allocated above (JR)
                            continue

                # Update performance counters (JR)
                self.parser_perf.add_timelog_event('parse line')
                self.parser_perf.increment_counter('parse line')

            # Write any remaining data to disk (JR)
            if len(self.products) > 0 or len(self.categories) > 0 or len(self.reviews) > 0:
                # self.dump_neo4j_db_csvs(batch_id=str(batch_idx).zfill(6))
                # testing batch output as json
                self.dump_json(batch_id=str(batch_idx).zfill(6))

            self.parser_perf.add_timelog_event('end')
            self.parser_perf.log_all()

        return

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

    def log_export_timestamp(self):
        export_history_path = os.path.join(project_root, 'var', 'logs')
        export_history_log = os.path.join(export_history_path, 'export_history.log')

        if not os.path.exists(export_history_path):
            os.makedirs(export_history_path)

        with open(export_history_log, 'a', 1, 'utf-8') as hl:
            hl.write(self.datestamp + '\n')

        return

    def export_neo4j_db_csv(self, dataset_name=str(), include_summary=False, batch_id=None):
        '''
        Writes a single header/csv set of files for nodes and edges of the selected dataset.
        If the header already exists for the given name_base and self.datestamp, one is not created and data is appended to the corresponding data csv file (JR)
        '''

        dirpath = os.path.join(self.data_repo, 'csv_batches', self.datestamp)

        if not os.path.exists(dirpath):
            os.mkdir(dirpath)

        filepaths = dict()

        # Throw an error if no dataset is specified
        if len(dataset_name) == 0:
            raise Exception('Dataset to export not specified.')
        elif dataset_name not in self.export_vars:
            raise Exception('Unknown dataset %(name)s.  Expected one of the following: %(opts)s' % {'name': dataset_name, 'opts': ', '.join(self.export_vars)})

        # If no name is provided, construct one using the requested dataset (JR)
        # if len(name_base) == 0:
            # name_base = 'n4db_%(idx)s' %{'idx': len([x for x in os.listdir(dirpath) if x.startswith('n4db') and 'header' in x])}

        name_base = 'n4db_%(dsn)s' % {'dsn': dataset_name}

        header_maps = {
            'node': {
                'product'   : '\t'.join([
                    'ASIN:ID(asin_id)', 'title:string', 'group:string', 'salesrank:long', 'similar_to_ct:int',
                    'category_path_ct:int', 'category_path_depth_avg:float', 'category_path_depth_sd:float',
                    'review_ct:int', 'review_downloaded_ct:int', 'review_rating_avg:float', 'review_rating_sd:float',
                    'review_votes_total:int', 'review_votes_avg:float', 'review_votes_sd:float', 'review_mttr:float',
                    'review_helpful_ratio_avg:float', 'review_helpful_ratio_sd:float',
                    'review_rating_avg_wtd:float', 'review_rating_wtd_avg:float', 'review_rating_wtd_sd:float', 'customers_unique_ct:int', ':LABEL'
                ]),
                'category'  : '\t'.join(['Id:ID(cat_id)', 'path:string', 'path_depth:int', ':LABEL']),
                'review'    : '\t'.join(['Id:ID(rev_id)', 'review_date:date', 'customer:string', 'rating:int', 'votes:int', 'helpful:int', 'helpful_ratio:float', 'review_rating_wtd:float', ':LABEL']),
                'customer'  : '\t'.join([
                    'Id:ID(cust_id)', 'review_ct:int', 'review_mttr:float', 'helpful_avg:float', 'helpful_sd:float', 'rating_avg:float', 'rating_sd:float',
                    'rating_wtd:float', 'votes_total:int', 'votes_avg:float', 'votes_sd:float', 'helpful_ratio_avg:float', 'helpful_ratio_sd:float', ':LABEL'
                ])
            },
            'edge': {
                'product'   : '\t'.join([':START_ID(asin_id)', ':END_ID(asin_id)', ':TYPE']),
                'category'  : '\t'.join([':START_ID(asin_id)', ':END_ID(cat_id)', ':TYPE']),
                'review'    : '\t'.join([':START_ID(asin_id)', ':END_ID(rev_id)', ':TYPE']),
                'customer'  : '\t'.join([':START_ID(asin_id)', ':END_ID(rev_id)', ':TYPE'])
            },
            'summary': {
                'product'   : '\t'.join(['review_ct:int', 'review_ct_avg:float', 'review_ct_sd:float', ':LABEL']),
                'category'  : '\t'.join(['path_depth_avg:float', 'path_depth_sd:float', ':LABEL']),
                'review'    : '\t'.join(['products_reviewed:int', 'review_unique_customers:int', ':LABEL']),
                'customer'  : '\t'.join(['review_ct_avg:float', 'review_ct_sd:float', 'votes_avg:float', 'votes_sd:float', 'helpful_ratio_avg:float', 'helpful_ratio_sd:float', ':LABEL'])
            }
        }

        # Product nodes & edges
        # filepaths_node_header = {name:os.path.join(dirpath, 'n4db_%(n)s_node_header_%(ds)s.csv' % {'n': name, 'ds': self.datestamp}) for name in self.export_vars}
        # filepaths_node_data = {name:os.path.join(dirpath, 'n4db_%(n)s_node_data_%(ds)s.csv' % {'n': name, 'ds': self.datestamp}) for name in self.export_vars}

        file_labels = [item for sublist in [['_'.join([x,y]) for x in ['node', 'edge']] for y in ['header', 'data']] for item in sublist]

        filepaths = {
            'node': {
                'header': os.path.join(dirpath, '%(base)s_node_header.csv' % {'base': name_base}) #,
                # 'header': os.path.join(dirpath, '%(base)s_node_header_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp}) #,
                # 'data': os.path.join(dirpath, '%(base)s_node_data_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp})
            },
            'edge': {
                'header': os.path.join(dirpath, '%(base)s_edge_header.csv' % {'base': name_base}) #,
                # 'header': os.path.join(dirpath, '%(base)s_edge_header_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp}) #,
                # 'data': os.path.join(dirpath, '%(base)s_edge_data_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp})
            },
            'summary': {
                'header': os.path.join(dirpath, '%(base)s_summary_header.csv' % {'base': name_base})
            }
        }

        if batch_id is None:
            filepaths['node']['data'] = os.path.join(dirpath, '%(base)s_node_data.csv' % {'base': name_base})
            filepaths['edge']['data'] = os.path.join(dirpath, '%(base)s_edge_data.csv' % {'base': name_base})
            filepaths['summary']['data'] = os.path.join(dirpath, '%(base)s_summary_data.csv' % {'base': name_base})
            # filepaths['node']['data'] = os.path.join(dirpath, '%(base)s_node_data_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp})
            # filepaths['edge']['data'] = os.path.join(dirpath, '%(base)s_node_data_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp})
        else:
            filepaths['node']['data'] = os.path.join(dirpath, '%(base)s_node_data_%(bid)s.csv' % {'base': name_base, 'bid': batch_id})
            filepaths['edge']['data'] = os.path.join(dirpath, '%(base)s_edge_data_%(bid)s.csv' % {'base': name_base, 'bid': batch_id})
            filepaths['summary']['data'] = os.path.join(dirpath, '%(base)s_summary_data_%(bid)s.csv' % {'base': name_base, 'bid': batch_id})
            # filepaths = {
            #     'node': {
            #         'header': os.path.join(dirpath, '%(base)s_node_header_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp}) #,
            #         # 'data': os.path.join(dirpath, '%(base)s_node_data_%(ds)s_%(bid)s.csv' % {'base': name_base, 'ds': self.datestamp, 'bid': batch_id})
            #     },
            #     'edge': {
            #         'header': os.path.join(dirpath, '%(base)s_edge_header_%(ds)s.csv' % {'base': name_base, 'ds': self.datestamp}) #,
            #         # 'data': os.path.join(dirpath, '%(base)s_edge_data_%(ds)s_%(bid)s.csv' % {'base': name_base, 'ds': self.datestamp, 'bid': batch_id})
            #     },
            # }

        #TODO: split node and data writes into separate functions to be run in parallel (JR)
        if not os.path.isfile(filepaths['node']['header']):
            with open(filepaths['node']['header'], 'w', 1, 'utf-8') as csv:
                csv.write(header_maps['node'][dataset_name])

        if not os.path.isfile(filepaths['edge']['header']):
            with open(filepaths['edge']['header'], 'w', 1, 'utf-8') as csv:
                csv.write(header_maps['edge'][dataset_name])

        if include_summary and not os.path.isfile(filepaths['summary']['header']):
            with open(filepaths['summary']['header'], 'w', 1, 'utf-8') as csv:
                csv.write(header_maps['summary'][dataset_name])


        with open(filepaths['node']['data'], 'a', 1, 'utf-8') as csv:
            print('Generating %(ds)s nodes.' % {'ds': dataset_name})
            match dataset_name:
                case 'product':
                    for product in self.products:
                        # output = '\t'.join([product, '\t'.join([str(x[1]).strip() for x in self.products[product].items() if not x[0].startswith('similar')]), 'PRODUCT\n'])
                        # Attempting to work around needing to map ASINs to IDs across multiple files by using ASINs as the node ID (JR)
                        output = '\t'.join(['\t'.join([str(y).strip() for x,y in self.products[product].items() if not x == 'similar_to']), 'PRODUCT\n'])
                        csv.write(output)

                case 'category':
                    for product in self.categories:
                        for cat_id in self.categories[product]:
                            output = '\t'.join([cat_id, '\t'.join([str(x) for x in self.categories[product][cat_id].values()]), 'CATEGORY\n'])
                            csv.write(output)

                case 'review':
                    for product_id in self.reviews:
                        for rev_id in self.reviews[product_id]:
                            output = '\t'.join([rev_id, '\t'.join([str(x).strip() for x in self.reviews[product_id][rev_id].values()]), 'REVIEW\n'])
                            csv.write(output)

                case 'customer':
                    for customer_id in self.customers:
                        output = '\t'.join([customer_id, '\t'.join([str(y).strip() for x,y in self.customers[customer_id].items() if not x == 'reviews']), 'CUSTOMER\n'])
                        csv.write(output)

        with open(filepaths['edge']['data'], 'a', 1, 'utf-8') as csv:
            print('Generating %(ds)s edges.' % {'ds': dataset_name})
            match dataset_name:
                case 'product':
                    # Construct set of unique edges (JR)
                    edge_pairs = set('\t'.join([val['ASIN'], sim, 'IS_SIMILAR_TO\n']) for pid,val in self.products.items() for sim in val['similar_to'].split(';') if val['similar_to'] != '')
                    for pair in edge_pairs:
                        csv.write(pair)
                    # for product_id in self.products:
                    #     # Attempting to work around needing to map ASINs to IDs across multiple files by using ASINs as the node ID (JR)
                    #     #TODO: CHANGE WORKFLOW TO COLLECT EDGE DATA AS {'<ASIN>': '<id>'} AND {'<ASIN>': '<sim_list>'}} THEN REMAP ASINS TO IDS AFTER ALL DATA HAS BEEN PARSED?
                    #     for sim_asin in self.products[product_id]['similar_to'].split(';'):
                    #         if sim_asin != '' and sim_asin is not None:
                    #             csv.write('\t'.join([self.products[product]['ASIN'], sim_asin, 'IS_SIMILAR_TO\n']))
                case 'category':
                    #TODO: ENSURE catid INDEX PERSISTS ACROSS EXPORTS
                    #TODO: PREVENT DUPLICATE catid, path ENTRIES
                    edge_pairs = set('\t'.join([self.products[pid]['ASIN'], cid, 'CATEGORIZED_AS\n']) for pid,pval in self.categories.items() for cid in pval)
                    for pair in edge_pairs:
                        csv.write(pair)
                    # for product_id in self.categories:
                    #     for cat_id in self.categories[product_id].keys():
                    #         # csv.write('\t'.join([product_id, cat_id, 'CATEGORIZED_AS\n']))
                    #         csv.write('\t'.join([self.products[product_id]['ASIN'], cat_id, 'CATEGORIZED_AS\n']))
                case 'review':
                    edge_pairs = set('\t'.join([self.products[pid]['ASIN'], rid, 'REVIEWED_BY\n']) for pid,pval in self.reviews.items() for rid in pval)
                    for pair in edge_pairs:
                        csv.write(pair)
                    # for product_id in self.reviews:
                    #     for rev_id in self.reviews[product_id].keys():
                    #         # csv.write('\t'.join([product_id, rev_id, 'REVIEWED_BY\n']))
                    #         csv.write('\t'.join([self.products[product_id]['ASIN'], rev_id, 'REVIEWED_BY\n']))
                case 'customer':
                    edge_pairs = set('\t'.join([cid, rid, 'WROTE_REVIEW\n']) for cid,cval in self.customers.items() for rid in cval['reviews'].split(';') if cval['reviews'] != '')
                    for pair in edge_pairs:
                        csv.write(pair)
                    # for customer_id, val in self.customers.items():
                    #     revs = val['reviews'].split(';')
                    #     for rev_id in revs:
                    #         csv.write('\t'.join([customer_id, rev_id, 'WROTE_REVIEW\n']))
                    #         #TODO: CREATE SEPARATE RELATION WITH PRODUCT IDS INCLUDED

        if include_summary:
            with open(filepaths['summary']['data'], 'w', 1, 'utf-8') as sf:
                output = '\t'.join(['\t'.join([str(y) for x,y in self.summaries[dataset_name].items()]), '_'.join(['SUMMARY', dataset_name.upper()])])
                sf.write(output)

    def dump_neo4j_db_csvs(self, batch_id=None):
        # Exporting all datasets
        for ds in self.export_vars:
            self.export_neo4j_db_csv(ds, batch_id)

            # Freeing up memory now that no further work will be done with the current dataset (JR)
            self.clear_datasets(ds)

        self.log_export_timestamp()

    def dump_json(self, batch_id = None):
        for subset in self.export_vars:
            output_path = os.path.join(self.data_repo, 'json_batches', self.datestamp)
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            with open(os.path.join(output_path, '%(s)s_%(bid)s.json' % {'s': subset, 'bid': batch_id}), 'w', 1, 'utf-8') as f:
                match subset:
                    case 'product':
                        json.dump(self.products, f)
                        self.products = dict()
                    case 'category':
                        json.dump(self.categories, f)
                        self.categories = dict()
                    case 'review':
                        json.dump(self.reviews, f)
                        self.reviews = dict()
                    case 'customer':
                        json.dump(self.customer_history, f)
                        self.customer_history = dict()
                    case _:
                        continue

        self.log_export_timestamp()

        return

    def collate_data(self, timestamp, subset):
        collated_data = dict()
        repo_path = os.path.join(self.data_repo, 'json_batches', timestamp)
        repo_files = os.listdir(repo_path)

        for f in [x for x in repo_files if x.startswith(subset)]:
            batch_id = re.findall('(?<=_)\d+(?=\.json)', f)[0]

            if batch_id not in collated_data.keys():
                collated_data[batch_id] = dict()

            with open(os.path.join(repo_path, f), 'r', 1, 'utf-8') as collection:
                collated_data[batch_id] = json.load(collection)

        # Sort the batches once collated (JR)
        collated_data = {x:collated_data[x] for x in sorted(collated_data)}
        # Remove the outer dictionary layer (JR)
        collated_data = {b:c for a in collated_data for b,c in collated_data[a].items()}

        return collated_data

    def export_with_summary(self, subset):

        match subset:
            case 'product':
                # Only considering those items which actually have reviews associated with them (JR)
                review_counts = [int(y['review_total_ct']) for x,y in self.products.items() if len(y['review_total_ct']) > 0]
                unique_customer_counts = [0 if y['customers_unique'] == '' else int(y['customers_unique']) for x,y in self.products.items()]

                self.summaries['product'] = {
                    'review_ct'     : int(np.sum(review_counts)),
                    'review_ct_avg' : round(np.mean(review_counts), self.precision),
                    'review_ct_sd'  : round(np.std(review_counts), self.precision)
                }

            case 'category':
                path_depths = [y[z]['path_depth'] for x,y in self.categories.items() for z in y]

                self.summaries['category'] = {
                    'path_depth_avg': round(np.mean(path_depths), self.precision),
                    'path_depth_sd' : round(np.std(path_depths), self.precision)
                }

            case 'review':
                customer_ids = set(y[z]['customer'] for x,y in self.reviews.items() for z in y)

                self.summaries['review'] = {
                    'products_reviewed'         : len([y[z] for x,y in self.categories.items() for z in y]),
                    'review_unique_customers'   : len(customer_ids)
                }

            case 'customer':
                # customer_ids = [x for x in self.customers]
                # customers = {x:{} for x in customer_ids}
                # customers = dict()
                customer_review_cts = list()
                customer_vote_cts = list()
                customer_helpful_ratios = list()

                for cid in self.customers:
                    review_dates = list()
                    helpful_resp = list()
                    ratings = list()
                    votes = list()
                    helpful_ratios = list()

                    if cid not in self.customers.keys():
                        self.customers[cid] = dict()

                    # for rev in self.customers[cid]:
                    #     review_dates.append(datetime.strptime(self.customers[cid][rev]['review_date'], '%Y-%m-%d'))
                    #     helpful_resp.append(int(self.customers[cid][rev]['helpful']))
                    #     ratings.append(int(self.customers[cid][rev]['rating']))
                    #     votes.append(int(self.customers[cid][rev]['votes']))
                    #     helpful_ratios.append(int(self.customers[cid][rev]['helpful_ratio']))

                    review_dates    = [datetime.strptime(y['review_date'], '%Y-%m-%d') for x,y in self.customers[cid].items()]
                    days_between_reviews = [x.days for x in np.ediff1d([d for d in sorted(review_dates)])]
                    helpful_resp    = [int(y['helpful']) for x,y in self.customers[cid].items()]
                    ratings         = [int(y['rating']) for x,y in self.customers[cid].items()]
                    votes           = [int(y['votes']) for x,y in self.customers[cid].items()]
                    helpful_ratios  = [float(y['helpful_ratio']) for x,y in self.customers[cid].items()]

                    self.customers[cid] = {
                        'reviews'           : ';'.join([x for x in self.customers[cid]]),
                        'review_ct'         : len(review_dates),
                        'review_mttr'       : 0 if len(days_between_reviews) == 0 else round(np.mean(days_between_reviews), self.precision),
                        'helpful_avg'       : round(np.mean(helpful_resp), self.precision),
                        'helpful_sd'        : round(np.std(helpful_resp), self.precision),
                        'rating_avg'        : round(np.mean(ratings), self.precision),
                        'rating_sd'         : round(np.std(ratings), self.precision),
                        'rating_wtd'        : 0 if helpful_ratios[0] in ['', 0] else round(np.dot(ratings, helpful_ratios)/np.sum(helpful_ratios), self.precision),
                        'votes_total'       : int(np.sum(votes)),
                        'votes_avg'         : round(np.mean(votes), self.precision),
                        'votes_sd'          : round(np.std(votes), self.precision),
                        'helpful_ratio_avg' : round(np.mean(helpful_ratios), self.precision),
                        'helpful_ratio_sd'  : round(np.std(helpful_ratios), self.precision)
                    }

                    # customer_review_cts.append(self.customers[cid]['review_ct'])
                    # customer_vote_cts.append(self.customers[cid]['votes_total'])
                    # customer_helpful_ratios.append(self.customers[cid]['helpful_ratio_avg'])

                customer_review_cts =       [self.customers[x]['review_ct'] for x in self.customers]
                customer_vote_cts =         [self.customers[x]['votes_total'] for x in self.customers]
                customer_helpful_ratios =   [self.customers[x]['helpful_ratio_avg'] for x in self.customers]

                self.summaries['customer'] = {
                    'review_ct_avg'     : round(np.mean(customer_review_cts), self.precision),
                    'review_ct_sd'      : round(np.std(customer_review_cts), self.precision),
                    'votes_avg'         : round(np.mean(customer_vote_cts), self.precision),
                    'votes_sd'          : round(np.std(customer_vote_cts), self.precision),
                    'helpful_ratio_avg' : round(np.mean(customer_helpful_ratios), self.precision),
                    'helpful_ratio_sd'  : round(np.std(customer_helpful_ratios), self.precision)
                }

            # case _:
        self.export_neo4j_db_csv(dataset_name=subset, include_summary=True)

    def merge(self, timestamp=None, subset=None):
        if timestamp is None:
            timestamp = self.get_latest_export_timestamp()
        
        if subset is not None:
            # Collate and combine batches (JR)
            match subset:
                case 'product':
                    self.products = self.collate_data(timestamp, subset)
                case 'category':
                    self.categories = self.collate_data(timestamp, subset)
                case 'review':
                    self.reviews = self.collate_data(timestamp, subset)
                case 'customer':
                    self.customers = self.collate_data(timestamp, subset)

            self.export_with_summary(subset)

        else:
            raise Exception('Dataset to merge not specified.')

    def similar_asin_to_id(self):
        # translates ASIN values to ids generated during parsing, adds new property to product metadata
        base_map = {self.products[x]['ASIN']:x for x in self.products}
        for product in self.products:
            sim_asins = self.products[product]['similar_to']
            if sim_asins != '':
                self.products[product]['similar_to_ids'] = ';'.join([base_map[x] for x in sim_asins.split(';') if x in base_map.keys()])


class ParseAsync():
    def __init__(self):
        self.results = list()
        self.process_cap = 1 if cpu_count() == 1 else cpu_count() - 1


    def load_split(self, parser, i, file):
        # Wrapper for Parser.load_split() which enables for passing back of an iteration number for Pool.apply_async()
        parser.load_split(file)
        return i

    def parse_async_apply(self, files):
        pool = Pool(self.process_cap)
        parser = Parser()

        # Initial parsing phase
        try:
            for i, f in enumerate(files):
                pool.apply_async(self.load_split, args=(parser, i, f), callback=self.collect_results)
        finally:
            pool.close()
            # Wait until all processes have finished
            pool.join()

        # Postprocessing phase
        
        return
    
    def parse_async_map(self, files):
        pool = Pool(self.process_cap)
        parser = Parser()

        # Initial parsing phase
        try:
            tmp_results = pool.starmap_async(self.load_split, [(parser, i, f) for i, f in enumerate(files)], callback=self.collect_results)
            self.results = tmp_results.get()
        finally:
            pool.close()
            # Wait until all processes have finished
            pool.join()

        # Postprocessing phase
        
        return

    def collect_results(self, result):
        self.results.append(result)


def main(mode='parse'):
    parser = Parser()

    if mode == 'parse':
        print('parsing data from amazon-meta.txt')
        parser.load_batched(os.path.join(project_root, 'data', 'amazon-meta.txt'))
        print('creating ASIN map for similar products')
        parser.similar_asin_to_id()

        print('exporting restructured data as json')
        parser.export_json_all()

        print('exporting as Neo4j db components for "neo4j-admin import database"')
        parser.export_neo4j_db_csv()

    elif mode == 'parse_split_single':
        parser.load_split(os.path.join(project_root, 'data', 'split_data', '000000.txt'))

    elif mode == 'parse_batch':
        # Untested
        # delayed_fns = [delayed(parser.load_split(os.path.join(project_root, 'data', 'split_data')))]
        # parallel_pool = Parallel(n_jobs=(cpu_count() - 1))
        # parallel_pool(delayed_fns)

        pool = Pool((cpu_count() - 1))
        try:
            results = [pool.apply(parser.load_split, args=(os.path.join(project_root, 'data', 'split_data', f),)) for f in os.listdir(os.path.join(project_root, 'data', 'split_data'))]
        finally:
            pool.close()


    elif mode == 'parse_async_apply':
        print('Parsing source data file via apply_async.')
        async_parser = ParseAsync()
        async_parser.parse_async_apply([os.path.join(project_root, 'data', 'split_data', f) for f in os.listdir(os.path.join(project_root, 'data', 'split_data'))])

    elif mode == 'parse_async_map':
        print('Parsing source data file.')
        async_parser = ParseAsync()
        async_parser.parse_async_map([os.path.join(project_root, 'data', 'split_data', f) for f in os.listdir(os.path.join(project_root, 'data', 'split_data'))])
    
    elif mode == 'merge':
        for ds in parser.export_vars:
            print('Collating and exporting %(ds)s data.' % {'ds': ds})
            parser.merge(timestamp='20221205_222022', subset=ds)

    elif mode == 'convert':
        # Importing prior export from JSON (JR)
        print('importing prior export')
        parser.import_json_all(timestamp='20221205_192223')

        print('exporting as Neo4j db components for "neo4j-admin import database"')
        parser.export_neo4j_db_csv()

    elif mode == 'split':
        parser.split_file(filename=os.path.join(project_root, 'data', 'amazon-meta.txt'))

    elif mode == 'upload':
        # Testing sending over the full dataset after extraction instead of inline (may incur additional overhead, need to test) (JR)
        # Moved dict iteration into separate function in N4J to capture performance stats
        n4_loader = N4J()

        # edges
        adjset = parser.returnsimilar(dataset)

        n4_loader.add_edges(adjset, 'IS_SIMILAR_TO')
        n4_loader.close()

   
if __name__ == "__main__":
    # Stage 1: Initial parsing and structuring of data (JR)
    #   Breaks up source file into topic-specific groups split into smaller file sizes (JR)
    # Works; completes in ~8m36s
    main('split')

    # Test
    # main('parse_split_single')

    # Works
    # main('parse_batch')

    # Stage 2: Asynchronously parse structured files (JR)
    # Works; completes in ~190s for just product nodes and edges
    # ~3m47s for nodes/edges for products, categories, & reviews
    # ~3m38s - same
    # ~4m08s - same
    main('parse_async_apply')

    # Stage 3: Collate, merge, postprocessing, and summarization of data (JR)
    #   Also generates Customer node data derived from Review node data and provides deduplication (JR)
    # ~21m52s for json dump of products, categories, reviews, & customer histories
    # ~28m50s - same
    main('merge')

    # Failing on cat1073, not seeing how to fix it
    # main('parse_async_map')
