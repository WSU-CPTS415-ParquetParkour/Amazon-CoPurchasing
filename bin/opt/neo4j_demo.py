#! /usr/bin/python3

import os
import re
import sys
import configparser as cfg
from neo4j import GraphDatabase as gdb

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

config = cfg.ConfigParser()
config.read(config_path)

class Example:
    def __init__(self):
        self.endpoint = ''.join(['bolt://', config.get('database_connection', 'dbhost'), ':', config.get('database_connection', 'dbport')])
        self.driver = gdb.driver(
            self.endpoint,
            auth=(config.get('database_connection', 'dbuser'), config.get('database_connection', 'dbpass'))
        )
    
    def close(self):
        self.driver.close()
    
    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)
    
    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run(
            "CREATE (a:Greeting) "
            "SET a.message = $message "
            "RETURN a.message + ', from node ' + id(a)",
            message=message
        )
        return result.single()[0]

if __name__ == '__main__':
    greeter = Example('bolt://10.0.1.42:7687', 'neo4j', 'neo4j')
    greeter.print_greeting('hello world')
    greeter.close()
