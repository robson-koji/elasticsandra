from elasticsandra.es_cassandra import CassandraReader
#from elasticsandra.es_elasticsearch import ElasticsearcReader

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
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'

        # self.stdout_path = '/dev/null'
        # self.stderr_path = '/dev/null'

        self.pidfile_path = '/tmp/mydaemon.pid'
        self.pidfile_timeout = 5

    def run(self):
        while True:
			cassandra_reader = CassandraReader(self.objects_dict)
			cassandra_reader.read_cassandra()
			time.sleep(float(self.delay))


app = App(delay)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()			