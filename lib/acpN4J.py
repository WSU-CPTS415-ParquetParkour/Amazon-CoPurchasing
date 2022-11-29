#! /usr/bin/python3

import os
import re
import configparser as cfg
from neo4j import GraphDatabase as gdb
from neo4j.exceptions import ServiceUnavailable

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

config = cfg.ConfigParser()
config.read(config_path)

class N4J:
    def __init__(self):
        self.endpoint = ''.join(['bolt://', config.get('database_connection', 'dbhost'), ':', config.get('database_connection', 'dbport')])
        self.driver = gdb.driver(
            self.endpoint,
            auth=(config.get('database_connection', 'dbuser'), config.get('database_connection', 'dbpass'))
        )

    def close(self):
        self.driver.close()
    
    # Derived from https://neo4j.com/docs/python-manual/current/get-started/ (JR)
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getlLogger('neo4j').addHandler(handler)
        logging.getLogger('neo4j').setLevel(level)

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

    def get_edge_types(self):
        with self.driver.session() as session:
            result = session.execute_read(self._get_acp_n4_edge_types)
        return result

    @staticmethod
    def _get_acp_n4_edge_types(transaction):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH ()-[r]->()',
            'RETURN TYPE(r) AS rel_type, COUNT(r) AS rel_type_n',
            'ORDER BY rel_type_n DESC;'
        ])
        result = transaction.run(cypher)

        try:
            return [{
                'rel_type': row['rel_type'],
                'rel_type_n': row['rel_type_n']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

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


#TODO: Just for temporary testing, will need to be removed when ready to be sourced by other files (JR)
def main():
    n4 = N4J()

    try:
        edge_types = n4.get_edge_types()
    finally:
        n4.close()

    return

if __name__ == "__main__":
    main()