import os
import re
import logging
import configparser as cfg
import pandas as pd
import numpy as np
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
        self.default_query_limit = int(config.get('app', 'default_query_limit'))

    def close(self):
        self.driver.close()
    
    # Derived from https://neo4j.com/docs/python-manual/current/get-started/ (JR)
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getlLogger('neo4j').addHandler(handler)
        logging.getLogger('neo4j').setLevel(level)

    def add_indices(self):
        with self.driver.session() as session:
            result = session.execute_write(self._add_indices)
        return

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

    def get_user_product_ratings(self):
        with self.driver.session() as session:
            result = session.execute_read(self._get_user_product_ratings)
            result = pd.pivot_table(pd.DataFrame(result), values='rating', index='asin', columns='cust_id').replace(np.nan, 0)
        return result
    
    def get_random_customer_node(self, rating_lower=0, review_ct_lower=1):
        with self.driver.session() as session:
            result = session.execute_read(self._get_random_customer_node, rating_lower, review_ct_lower)
            result = result[0]
        return result

    def get_product_groups(self):
        with self.driver.session() as session:
            result = session.execute_read(self._get_product_groups)
        return result

    def get_product_categories(self):
        with self.driver.session() as session:
            result = session.execute_read(self._get_product_categories)
        return result

    def get_products_in_groups(self, group_list):
        with self.driver.session() as session:
            result = session.execute_read(self._get_products_in_groups, group_list)
        return result

    def get_products_in_categories(self, category_list):
        with self.driver.session() as session:
            result = session.execute_read(self._get_products_in_categories, category_list)
        return result
    
    def get_user_product_groups(self, user_id):
        with self.driver.session() as session:
            result = session.execute_read(self._get_user_product_groups, user_id)
            result = list(set(result))
        return result

    def get_user_product_categories(self, user_id):
        with self.driver.session() as session:
            result = session.execute_read(self._get_user_product_categories, user_id)
        return result

    def get_user_product_groups_and_categories(self, user_id):
        with self.driver.session() as session:
            result = session.execute_read(self._get_user_product_groups_and_categories, user_id)
            result = {
                'group'     : list(set(x['group'] for x in result)),
                'category'  : list(set(x['category'] for x in result))
            }
        return result
    
    def get_user_product_peer_groups_and_categories(self, user_id):
        with self.driver.session() as session:
            result = session.execute_read(self._get_user_product_peer_groups_and_categories, user_id)
        return result

    def get_cf_set_from_asins(self, asins, limit=None):
        if limit is None:
            limit = self.default_query_limit
        with self.driver.session() as session:
            result = session.execute_read(self._get_cf_set_from_asins, asins, limit)
            result = pd.pivot_table(pd.DataFrame(result), values='rating', index='asin', columns='cust_id').replace(np.nan, 0)
        return result

    def get_titles_from_asins(self, asins):
        with self.driver.session() as session:
            result = session.execute_read(self._get_titles_from_asins, asins)
            result = pd.DataFrame(result)
        return result

    def get_rating_greater(self, rating, operand, limit=None):
        if limit is None:
            limit = self.default_query_limit
        with self.driver.session() as session:
            result = session.execute_read(self._get_rating_greater, rating, operand, limit)
            result = pd.DataFrame(result)
        return result


    @staticmethod
    def _add_indices(transaction):
        # Add indices for nodes & properties if they don't already exist (JR)
        cyphers = [
            'CREATE TEXT INDEX idx_text_customer_id IF NOT EXISTS FOR (n:CUSTOMER) ON (n.Id);',
            'CREATE TEXT INDEX idx_text_product_asin IF NOT EXISTS FOR (n:PRODUCT) ON (n.ASIN);',
            'CREATE TEXT INDEX idx_text_product_group IF NOT EXISTS FOR (n:PRODUCT) ON (n.group);',
            'CREATE TEXT INDEX idx_text_product_title IF NOT EXISTS FOR (n:PRODUCT) ON (n.title);',
            'CREATE TEXT INDEX idx_text_review_id IF NOT EXISTS FOR (n:REVIEW) ON (n.Id);',
            'CREATE TEXT INDEX idx_text_review_customer IF NOT EXISTS FOR (n:REVIEW) ON (n.customer);',
            'CREATE TEXT INDEX idx_text_category_path IF NOT EXISTS FOR (n:CATEGORY) ON (n.path);'
        ]
        result = [transaction.run(c) for c in cyphers]


    @staticmethod
    def _get_acp_n4_edge_types(transaction):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are (JR)
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
        # Performance improvement?  Generate edges by all ASINs in 'similar' (JR)
        # May require the 'similar' set of ASINs added to each node?  Restructuring of dict? (JR)
        # Example:
            # MATCH (a:PRODUCT)
            # WHERE a.ASIN IN ['039474067X','0679730672','0679750541','1400030668','0896086704']
            # RETURN COUNT(a)
        # Mockup:
            # MATCH (a:PRODUCT), (b:PRODUCT)
            # WHERE b.ASIN IN a.similar
            # CREATE (a)-[:%(rel)s]->(b)

        # TODO: Add primary key for nodes; UNIQUE property (JR)
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are (JR)
        cypher = 'MATCH (a:PRODUCT), (b:PRODUCT) WHERE a.ASIN = \'%(from)s\' AND b.ASIN = \'%(to)s\' CREATE (a)-[:%(rel)s]->(b)' % {'from':src, 'to':dest, 'rel':relation}
        result = transaction.run(cypher)
        return

    @staticmethod
    def _load_acp_csv(transaction, csv):
        cypher = ''
        result = transaction.run(cypher)
        return

    @staticmethod
    def _get_rating_greater(transaction, rating, operand, limit=None):
        if limit is None:
            limit = self.default_query_limit
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (n:PRODUCT) WHERE n.review_rating_avg %(operand)s %(rating)s RETURN n.ASIN AS asin, n.title AS title ORDER BY n DESC LIMIT %(lim)s;'%{'operand':operand,'rating':rating, 'lim': limit}
            ])
        # print(cypher)
        result = transaction.run(cypher)

        try:
            return [{
                'asin'  : row['asin'],
                'title' : row['title']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    def get_similar_product(self,ASIN):
        with self.driver.session() as session:
            result = session.execute_read(self._get_similar_product,ASIN)
        return result

    @staticmethod
    def _get_similar_product(transaction, ASIN):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (:PRODUCT {ASIN: \'%(id)s\'})-[r:IS_SIMILAR_TO]->(product:PRODUCT) RETURN product.title AS TITLE, product.ASIN AS asin' % {'id': ASIN}
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
    def _get_num_reviews(transaction, ASIN):
        # Specify unique node id instead of letting neo4j define it - find out what the limitations of this are
        cypher = ' '.join([
            'MATCH (thing:PRODUCT {ASIN:\'%(id)s\'}) RETURN thing.review_ct AS Review_Count, thing.title AS Title' % {'id': ASIN}
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
    def _get_user_product_ratings(transaction, limit=None):
        if limit is None:
            limit = self.default_query_limit
        cypher = 'MATCH (a:REVIEW)<-[:REVIEWED_BY]-(b) RETURN a.customer AS cust_id, b.ASIN as asin, a.rating AS rating LIMIT %(lim)s;' % {'lim': limit}
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

    # TODO: FINISH THIS (JR)
    @staticmethod
    def _get_user_product_peer_ratings(transaction):
        cypher = ''

    @staticmethod
    def _get_random_customer_node(transaction, rating_lower_limit=0, review_ct_lower_limit=1):
        cypher = 'MATCH (a:CUSTOMER) WHERE a.rating_avg > %(rll)s AND a.review_ct > %(rcll)s RETURN a, rand() AS r ORDER BY r LIMIT 1;' % {'rll': rating_lower_limit, 'rcll': review_ct_lower_limit}
        result = transaction.run(cypher)

        try:
            return [row[0]['Id'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_product_groups(transaction):
        cypher = 'MATCH (a:PRODUCT) RETURN a.group AS group, COUNT(a) as n;'
        result = transaction.run(cypher)

        try:
            return [row['group'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_product_categories(transaction):
        cypher = 'MATCH (a:CATEGORY) RETURN a.path AS path, COUNT(a) as n;'
        result = transaction.run(cypher)

        try:
            return [row['path'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_products_in_groups(transaction, grp_list):
        cypher = 'MATCH (a:PRODUCT) WHERE a.group IN [%(gl)s] RETURN a.ASIN as asin;' % {'gl': ','.join([x for x in grp_list])}
        result = transaction.run(cypher)

        try:
            return [row['asin'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_products_in_categories(transaction, cat_list):
        cypher = 'MATCH (a:PRODUCT)-[:CATEGORIZED_AS]->(b:CATEGORY) WHERE b.path IN [%(cl)s] RETURN a.ASIN AS asin;' % {'cl': ','.join([x for x in cat_list])}
        result = transaction.run(cypher)

        try:
            return [row['asin'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_user_product_groups(transaction, usr_id):
        cypher = 'MATCH (a:CUSTOMER)-->(:REVIEW)<--(b:PRODUCT) WHERE a.Id = \'%(uid)s\' RETURN b.group AS group;' % {'uid': usr_id}
        result = transaction.run(cypher)

        try:
            return [row['group'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_user_product_categories(transaction, usr_id):
        cypher = 'MATCH (a:CUSTOMER)-->(:REVIEW)<--(:PRODUCT)-->(b:CATEGORY) WHERE a.Id = \'%(uid)s\' RETURN b.path AS category;' % {'uid': usr_id}
        result = transaction.run(cypher)

        try:
            return [row['category'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_user_product_groups_and_categories(transaction, usr_id):
        cypher = 'MATCH p=(a:CUSTOMER)-[r:WROTE_REVIEW]->(:REVIEW)<-[:REVIEWED_BY]-(b:PRODUCT)-[:CATEGORIZED_AS]->(c:CATEGORY) WHERE a.Id = \'%(uid)s\' RETURN b.group AS group, c.path AS path' % {'uid': usr_id}
        result = transaction.run(cypher)

        try:
            return [{
                'group': row['group'],
                'category': row['path']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_user_product_peers(transaction, usr_id):
        cypher = 'MATCH p=(a:CUSTOMER)-->(:REVIEW)<--(:PRODUCT)-->(:REVIEW)<--(b:CUSTOMER) WHERE a.Id = \'%(uid)s\' AND b.Id <> \'%(uid)s\' RETURN b.Id AS peer_id' % {'uid': usr_id}
        result = transaction.run(cypher)

        try:
            return [row['category'] for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_user_product_peer_groups_and_categories(transaction, usr_id):
        cypher = 'MATCH (:CUSTOMER)-->(:REVIEW)<--(:PRODUCT)-->(:REVIEW)<--(:CUSTOMER)-->(:REVIEW)<--(a:PRODUCT)-->(b:CATEGORY) WHERE a.Id = \'%(uid)s\' AND b.Id <> \'%(uid)s\' RETURN a.group AS peer_grp, b.path AS peer_cat;' % {'uid': usr_id}
        result = transaction.run(cypher)

        try:
            return [{
                'group': row['peer_grp'],
                'category': row['peer_cat']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise

    @staticmethod
    def _get_cf_set_from_subquery(transaction, base_query, base_limit=None):
        if base_limit is None:
            limit = self.default_query_limit
        # Receiving a query so that this can be run as a separate process in parallel to the one displaying product details from the query (JR)
        # base_query must return product ASIN aliased as asins (JR)

        # // take arbitrary product query, find all customers
        # CALL {
        #     MATCH (a:PRODUCT) WHERE a.review_mttr < 10
        #     RETURN a.ASIN AS inner_asins LIMIT 100
        # }
        # WITH inner_asins
        # // get product/customer/ratings edgelist for those customers
        # MATCH (a:PRODUCT)-->(b:REVIEW) WHERE a.ASIN IN [inner_asins]
        # RETURN a.ASIN AS asin, b.customer AS user_id, b.rating as rating
        cypher = 'CALL {%(bq)s LIMIT %(bl)s} WITH asins MATCH (a:PRODUCT)-->(b:REVIEW) WHERE a.ASIN IN [asins] RETURN a.ASIN AS asin, b.customer AS cust_id, b.rating as rating' % {'bq': base_query, 'bl': base_limit}
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
    
    @staticmethod
    def _get_cf_set_from_asins(transaction, asins, limit=None):
        if limit is None:
            limit = self.default_query_limit
        # Variant of _get_cf_set_from_subquery which expects to receive a list of ASINs (JR)
        asins = asins[:limit]
        cypher = 'MATCH (a:PRODUCT)-->(b:REVIEW) WHERE a.ASIN IN [%(al)s] RETURN a.ASIN AS asin, b.customer AS cust_id, b.rating as rating' % {'al': '\'' + '\',\''.join(asins) + '\''}
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
    
    @staticmethod
    def _get_titles_from_asins(transaction, asins):
        cypher = 'MATCH (a:PRODUCT) WHERE a.ASIN IN [%(al)s] RETURN a.ASIN AS asin, a.title AS title' % {'al': '\'' + '\',\''.join(asins) + '\''}
        result = transaction.run(cypher)

        try:
            return [{
                'asin'  : str(row['asin']),
                'title' : row['title']
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error('{query} raised an error: \n {exception}'.format(query=cypher, exception=exception))
            raise


# Just for temporary testing, may be removed when ready to be sourced by other files (JR)
def main():
    n4 = N4J()
    nodes = ['CATEGORY', 'CUSTOMER', 'PRODUCT', 'REVIEW']

    try:
        # n4.add_indices()
        node_set = n4.get_rating_greater(rating='4', operand='>', limit=100)
        properties = {lbl: n4.get_node_properties(lbl) for lbl in nodes}
        # with open(os.path.join(project_root, 'etc', 'node_property_keys.json'), 'w', 1, 'utf-8') as f: json.dump(properties, f)
        wtd_mtx = n4.get_user_product_ratings()
        cid = n4.get_random_customer_node()
        all_groups = n4.get_product_groups()
        all_categories = n4.get_product_categories()
        # user_peers = n4.get_user_product_peers(cid) #TODO
        user_groups = n4.get_user_product_groups(cid)
        user_cats = n4.get_user_product_categories(cid)
        user_grps_and_cats = n4.get_user_product_groups_and_categories(cid)

        groups_diff = set(all_groups).symmetric_difference(set(user_groups))
        cats_diff = set(all_categories).symmetric_difference(set(user_cats))

    finally:
        n4.close()

    return

if __name__ == "__main__":
    main()
