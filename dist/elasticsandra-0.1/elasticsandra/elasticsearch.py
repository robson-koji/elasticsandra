import uuid
import elasticsearch
from datetime import datetime

es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

class ElasticsearchLoader(object):
	def __init__(self, *args, **kwargs):
		self.index = kwargs['db']

	def insert_data(self, *args, **kwargs):
		print kwargs
		es.index(index=self.index, doc_type=kwargs['doc_type'], id=kwargs['id'], body=kwargs['es_columns'])




# dt = datetime.strptime("2015-02-14 17:48:38", "%Y-%m-%d %H:%M:%S")

# es_columns = {'timestamp': dt,
# 				'firstname': 'Jojo', 
# 				'lastname': 'Sobrenome xyz', 
# 				'age':  8, 
# 				'city': 'Maracatu', 
# 				'email': 'jojo@jojo.com'}

# es_init_kwargs = {'index': 'ccc'}

# #uuid = 'd2a5b9f9-cd3a-49d6-8269-fa8d2d881e0f'

# es_insert_kwargs = {'id': str(uuid.uuid4()),
# # es_insert_kwargs = {'id': uuid,
# 					'doc_type':'bbb',
# 					'es_columns': es_columns}

# es_loader = ElasticsearchLoader(**es_init_kwargs)
# es_loader.insert_data(**es_insert_kwargs)
					
#es_loader = ElasticsearchLoader()
#es_loader.load_elasticsearch()

# readElasticSearch()



