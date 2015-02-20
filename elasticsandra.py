from elasticsandra.es_cassandra import CassandraReader
from elasticsandra.es_elasticsearch import ElasticsearchReader

import os
import re
import sys
import time
import elasticsearch
import cassandra

from datetime import datetime
from daemon import runner


"""
To check arguments for delay execution repetions
"""
args = sys.argv
delay = 0

try:
    if sys.argv[1] == 'start':
        delay = int(sys.argv[2])
except IndexError, e:
    print "usage: python elasticsandra.py start <delay to repeat (in seconds)>"
    exit(0)
except Exception, e:
    print e
    exit(0)




class App():
    def __init__(self, delay):
        self.delay = delay
        self.objects_dict = {}

        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'

        #self.stdout_path = '/dev/null'
        #self.stderr_path = '/dev/null'

        self.pidfile_path = '/tmp/mydaemon.pid'
        self.pidfile_timeout = 5
        self.cassandra_reader = CassandraReader(self.objects_dict)
        self.elasticsearch_reader = ElasticsearchReader(self.objects_dict)

    def run(self):
        inicio = ''
        fim = ''
        while True:
            inicio = datetime.now()

            """
            Could open threads for each db
            """

            try:
                self.cassandra_reader.read_cassandra()
            except cassandra.cluster.NoHostAvailable as e:
                if re.search('Connection refused' , str(e)) is not None:
                    print """\n\n
                    ============================================
                    Cassandra is probably not running.
                    Start Cassandra first and restart the daemon
                    ============================================
                    \n\n"""
                    raise
                else:
                    print """\n\n
                    ================================================
                    Cassandra keyspaces not found. Restart Cassandra
                    sudo service cassandra restart
                    ================================================
                    \n\n"""
                    raise
            try:
                self.elasticsearch_reader.read_elasticsearch()
            except elasticsearch.exceptions.ConnectionError as e:
                print """\n\n
                ================================================
                Elasticsearch is probably not running.
                Start Elasticsearch first and restart the daemon
                ================================================
                \n\n"""
                raise
            except elasticsearch.exceptions.NotFoundError as e:
                print """\n\n
                ======================================
                ElasticSearch indices not found.
                Inject some data on Cassandra or 
                ElasticSerch to start synchronization.
                ======================================
                \n\n"""


            fim = datetime.now()
            print inicio
            print fim


            time.sleep(float(self.delay))





app = App(delay)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()           