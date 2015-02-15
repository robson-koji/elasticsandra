from es_cassandra import CassandraLoader
from es_elasticsearch import ElasticsearchLoader

"""
This list holds references to DB updater Classes,
which are called when data is new or is not up to date.
"""
data_origin_list = [ElasticsearchLoader, CassandraLoader]

"""
This Class checks whether data is new or is not up to date.
In both cases update respective DBs, otherwise just pass. 
"""
class TheChecker(object):
    def __init__(self, *args, **kwargs):    
        self.objects_dict = kwargs['objects_dict']
        self.caller = kwargs['caller']		
        not_caller_kwargs = {'db': kwargs['db']}
        
        # Works only while using two dbs, Elasticsearch and Cassandra.
        # If add more DBs to sync, need to change here.
        for not_caller in data_origin_list:
        	if not_caller != self.caller:
				# Instantiate the DB to update.
				self.updater = not_caller(**not_caller_kwargs)

    def check_exists(self, *args, **kwargs):
    	self.id = kwargs['id'] 
    	self.date = kwargs['timestamp'] 

    	if self.id not in self.objects_dict or (self.id in self.objects_dict and self.date > self.objects_dict[self.id]):
			# Update DB
			self.updater.insert_data(**kwargs)

			# Update dict item with actual date
			self.objects_dict[self.id] = self.date