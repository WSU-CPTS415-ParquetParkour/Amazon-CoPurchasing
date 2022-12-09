#! /usr/bin/python3

import os
import re
import logging
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
        result = list(set(row['rel_type'] for row in result))
        return result

    def get_node_properties(self, node_label):
        with self.driver.session() as session:
            result = session.execute_read(self._get_node_properties, node_label.upper())

        result = list(set(prop for row in result for y,prop_lst in row[node_label].items() for prop in prop_lst if prop != 'Id'))
        return result
    
    def get_edge_properties(self, edge_type):
        with self.driver.session() as session:
            result = session.execute_read(self._get_edge_properties, edge_type.upper())

        result = list(set(prop for row in result for y,prop_lst in row[node_label].items() for prop in prop_lst if prop != 'Id'))
        return result

    def get_num_reviews(self,ASIN):
        with self.driver.session() as session:
            result = session.execute_read(self._get_num_reviews,ASIN)
        return result

    def get_collab_filt_wt_adj_mtx(self):
        with self.driver.session() as session:
            result = session.execute_read(self._get_collab_filt_wt_adj_mtx)
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

    def get_rating_greater(self,rating,operand):
        with self.driver.session() as session:
            result = session.execute_read(self._get_rating_greater,rating,operand)
        return result    

    @staticmethod
    def _get_rating_greater(transaction,rating,operand):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (n:PRODUCT) WHERE n.review_rating_avg '+operand+' '+rating+' RETURN n.title AS title ORDER BY n DESC LIMIT 50;'
            ])
        print(cypher)
        result = transaction.run(cypher)

        try:
            return [{
                'title': row['title'],
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    def get_similar_product(self,ASIN):
        with self.driver.session() as session:
            result = session.execute_read(self._get_similar_product,ASIN)
        return result

    @staticmethod
    def _get_similar_product(transaction,ASIN):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (:PRODUCT {ASIN: \''+ASIN+'\'})-[r:IS_SIMILAR_TO]->(product:PRODUCT) RETURN product.title AS TITLE, product.ASIN AS asin'
            ])
        result = transaction.run(cypher)

        try:
            return [{
                'TITLE': row['TITLE'],
                'asin': row['asin']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_node_properties(transaction, node_label):
        cypher = 'MATCH (n:%(nl)s) RETURN KEYS(n) AS property_keys LIMIT 50;' % {'nl': node_label}
        result = transaction.run(cypher)
        try:
            return [{node_label: {
                    'properties': row['property_keys'],
                }
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_edge_properties(transaction, edge_type):
        cypher = 'MATCH ()-[r:%(et)s]->() RETURN KEYS(r) AS property_keys LIMIT 50;' % {'et': edge_type}
        result = transaction.run(cypher)
        try:
            return [{edge_type: {
                    'properties': row['property_keys'],
                }
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_num_reviews(transaction,ASIN):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (thing:PRODUCT {ASIN:\''+ASIN+'\'}) RETURN thing.review_ct AS Review_Count, thing.title AS Title'
            ])
        result = transaction.run(cypher)

        try:
            return [{
                'Review_Count': row['Review_Count'],
                'Title': row['Title']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_collab_filt_wt_adj_mtx(transaction):
        cypher = 'MATCH (a:REVIEW)<-[:REVIEWED_BY]-(b) RETURN a.customer AS cust_id, b.ASIN as asin, a.rating AS rating LIMIT 50;'
        result = transaction.run(cypher)

        try:
            return [{
                'cust_id': row['cust_id'],
                'asin': row['asin'],
                'rating': row['rating']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise










#TODO: Just for temporary testing, will need to be removed when ready to be sourced by other files (JR)
def main():
    n4 = N4J()
    nodes = ['CATEGORY', 'CUSTOMER', 'PRODUCT', 'REVIEW']

    try:
        node_set = n4.get_rating_greater(rating='4',operand='>')
        properties = {lbl: n4.get_node_properties(lbl) for lbl in nodes}
        # with open(os.path.join(project_root, 'etc', 'node_property_keys.json'), 'w', 1, 'utf-8') as f: json.dump(properties, f)
        wtd_mtx = n4.get_collab_filt_wt_adj_mtx()
    finally:
        n4.close()

    return

if __name__ == "__main__":
    main()