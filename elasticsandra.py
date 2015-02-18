from elasticsandra.es_cassandra import CassandraReader
from elasticsandra.es_elasticsearch import ElasticsearchReader

import os
import sys
import time

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
        # self.stdout_path = '/dev/tty'
        # self.stderr_path = '/dev/tty'

        self.stdout_path = '/dev/null'
        self.stderr_path = '/dev/null'

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
            self.elasticsearch_reader.read_elasticsearch()
            self.cassandra_reader.read_cassandra()


            fim = datetime.now()
            print inicio
            print fim


            time.sleep(float(self.delay))





app = App(delay)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()           