from datetime import datetime

import elasticsearch
import uuid

es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200



class ElasticsearchLoader(object):
	def __init__(self, *args, **kwargs):
		self.index = kwargs['db']

	def insert_data(self, *args, **kwargs):
		print kwargs
		es.index(index=self.index, doc_type=kwargs['doc_type'], id=kwargs['id'], body=kwargs['es_columns'])




class ElasticsearchReader(object):
#	from elasticsandra import TheChecker

	def readElasticSearch():
		es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

		# Get all Indices (Databases)
		#es_json = es.indices.get_settings(index='_all')
		try:
			es_json = es.indices.get_mapping(index='_all')
		except elasticsearch.exceptions.ConnectionError, e:
			raise
			exit(0)

		# print es_json
		# exit(0)

		# Get indices
		#print es_json.keys()

		# Indices (Database)
		for es_indice, indice_value in es_json.iteritems():
			print "Indice: %s" % es_indice
			# print indice_value

			# Types (Tables)
			for es_type, type_value in es_json[es_indice].get('mappings').iteritems():
				print "Type: %s" % es_type
				#print type_value
				#print es_json[es_indice].get('mappings')[es_type].get('properties')

				# Properties (Columns)
				# for es_property, property_value in es_json[es_indice].get('mappings')[es_type].get('properties').iteritems():
				# 	print "column: %s" % es_property
				# 	print property_value

				# Properties data
				es_hits = es.search(index=es_indice, doc_type=es_type).get('hits')
				for es_hits_hits in es_hits.get('hits'):
					print es_hits_hits
					# try:
					# 	print "_id: %s" % es_hits_hits.get('_id')
					# 	print dir(es_hits_hits.get('_source').get('timestamp'))
					# 	print es_hits_hits.get('_source').get('timestamp').__class__
					# 	print "timestamp: %s" % es_hits_hits.get('_source').get('timestamp')
					# except Exception, e:
					# 	print e
					# 	pass





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



