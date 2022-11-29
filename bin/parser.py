#! /usr/bin/python3

import os
import re
import configparser as cfg
import time
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
    # def __init__(self):

    def load(self, filename):
        self.parser_perf = PerfMon('Parser.load')
        self.parser_perf.add_timelog_event('init')
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

                # Update performance counters (JR)
                self.parser_perf.add_timelog_event('parse line')
                self.parser_perf.increment_counter('parse line')

            self.parser_perf.add_timelog_event('end')
            self.parser_perf.log_all()

        return data

    # list of adjacent ASINs
    def returnsimilar(self, data):
        adj = []
        for key in data:
            for item in data[key]['similar']:
                adj.append([int(key), item])
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
        # Thin wrapper around add_node() to capture timing data
        perf = PerfMon('N4J.add_node_set')
        perf.add_timelog_event('init')
        for node in node_dataset:
            self.add_node(node, node_dataset[node])
            perf.add_timelog_event('add node')
            perf.increment_counter('add node')
        perf.add_timelog_event('end')
        perf.log_all()

    @staticmethod
    def _create_acp_n4_node(transaction, idx, node_data):
        # Combine values by unpacking to get interpolation to work without additional string processing - merge operator ('|') only exists for Python 3.9+ (JR)
        # Need to prepend with a character since neo4j does not allow for node names to start with numbers: https://neo4j.com/docs/cypher-manual/current/syntax/naming/ (JR)
        # The node name can easily be replaced with another value, this is just to prototype the process before the data model is ready (JR)
        cypher = 'CREATE (%(idx)s:Product {ASIN:\'%(ASIN)s\', title:\'%(title)s\', group:\'%(group)s\', salesrank:\'%(salesrank)s\'})' % {**{'idx':'a_' + idx}, **node_data}
        result = transaction.run(cypher)
        return


def main():
    parser = Parser()
    #dict of raw data
    # dataset = load("sample.txt")
    # Ensuring that the path is constructed agnostic to the system which runs this (JR)
    dataset = parser.load(os.path.join(project_root, 'data', 'amazon-meta_first25.txt'))

    # Testing sending over the full dataset after extraction instead of inline (may incur additional overhead, need to test) (JR)
    # Moved dict iteration into separate function in N4J to capture performance stats
    n4_loader = N4J()
    n4_loader.add_node_set(dataset)
    n4_loader.close()

    # edges
    adjset = parser.returnsimilar(dataset)
    
    #to csv functions
    #rawdatatocsv(dataset, "rawdata.csv")
    #similartocsv(adjset, "similar.csv")
    print(dataset)
    
   
if __name__ == "__main__":
    main()