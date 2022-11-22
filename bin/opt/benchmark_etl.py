#! /usr/bin/python3

import os
import re
import shutil
import random
import configparser as cfg

from neo4j import GraphDatabase as gdb

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

config = cfg.ConfigParser()
config.read(config_path)

class N4Benchmark:
    def __init__(self):
        self.dataset = dict()
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

    def stage_data(self):
        file_list = os.listdir(os.path.join(project_root, 'data'))

        # Copy data to the local instnace
        if config.get('database_connection', 'dbhost') == 'localhost':
            for f in file_list:
                shutil.copy(f, os.path.join(os.environ['NEO4J_HOME'], 'import'))

        # Sends data to the server running the neo4j container
        else:
            print('Run the appropriate helper scripts in %(path)s to stage data on server.' % {'path': os.path.join(project_root, 'bin', 'opt')})

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
        cypher = 'CREATE (Product:\'%(lab)s\' {ASIN:\'%(ASIN)s\', title:\'%(title)s\', group:\'%(group)s\', salesrank:\'%(salesrank)s\'})' % {**{'lab':label}, **node_data}
        result = transaction.run(cypher)
        return
    
    @staticmethod
    def _create_acp_n4_edge(transaction, src, dest, relation):
        # TODO: Add primary key for nodes; UNIQUE property
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = 'MATCH (a:PRODUCT), (b:PRODUCT) WHERE a.ASIN = \'%(from)s\' AND b.ASIN = \'%(to)s\' CREATE (a)-[:%(rel)s]->(b)' % {'from':src, 'to':dest, 'rel':relation}
        result = transaction.run(cypher)
        return
    
    @staticmethod
    def _create_acp_n4_edge_group(transaction, src, dest_list, relation):
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

        group_list = ','.join(['\'%(x)s\'' for x in dest_list])
        cypher = 'MATCH (a:PRODUCT), (b:PRODUCT) WHERE a.ASIN = \'%(from)s\' AND b.ASIN IN %(togroup)s' % {'from': src, 'togroup': group_list}
        result = transaction.run(cypher)

    @staticmethod
    def _import_csv(trasaction, csv_name):
        cypher = 'USING PERIODIC COMMIT 500 LOAD CSV FROM \'file:///%(csv)s\' AS line CREATE (:PRODUCT), '
        result = transaction.run(cypher)

    def run(self, n_nodes, per_edges):

        benchmark_set = dict()
        for n in range(0, n_nodes):
            # related = random.sample([x for x in range(1,20)], random.randint(1,10))
            related = random.sample(benchmark_set, int(length(benchmark_set)/per_edges))
            # benchmark_set += {n: related}

            self.add_node(str(n), {})
        


        perf = PerfMon('N4J.load_csv')
        perf.add_timelog_event('init')
        for node in benchmark_set:
            self.add_node(node, benchmark_set[node])
        perf.add_timelog_event('end')
        perf.log_all()


def main():
    return


if __name__ == "__main__":
    main()