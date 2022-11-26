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