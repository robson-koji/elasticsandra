from datetime import datetime

import random, time_uuid
import elasticsearch



class ElasticsearchLoader(object):
	def __init__(self, *args, **kwargs):
		self.index = kwargs['db']
		self.es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

	def insert_data(self, *args, **kwargs):
		#print kwargs
		try:
			self.es.index(index=self.index, doc_type=kwargs['doc_type'], id=kwargs['id'], body=kwargs['es_columns'])
		except Exception as e:			
#			print kwargs
			print e



# Constant to limit number of register to synchronize each round.
# Delay for each round is defined on daemon initialization.
LIMIT = 1000

class ElasticsearchReader(object):
	def __init__(self, objects_dict):
		self.objects_dict = objects_dict


	def read_elasticsearch(self):
		from elasticsandra import TheChecker
		es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

		# Get all Indices (Databases)
		try:
			es_json = es.indices.get_mapping(index='_all')
		except elasticsearch.exceptions.ConnectionError, e:
			raise
			exit(0)

		# print es_json

		# Arguments to send to TheChecker
		tc_kwargs = {'objects_dict': self.objects_dict, 'caller': ElasticsearchLoader} 

		# Indices (Database)
		for es_indice, indice_value in es_json.iteritems():
			# if es_indice != "ahz3gng779mmzm1cnb1h":
			# 	continue

			# print "\n\n\n=============================="
			# print "Indice: %s" % es_indice


			# Instantiate TheCheker for each indice
			tc_kwargs['db'] = es_indice
			t_checker = TheChecker(**tc_kwargs)

			# Elasticsearch will models output data specific to Cassandra and vice versa.
			cs_insert_kwargs = {}

			# Types (Tables)
			for es_type, type_value in es_json[es_indice].get('mappings').iteritems():
				# if es_type != "ab12":
				# 	continue

				# print "\n\n\n\n\n\n\n--------------------"
				# print "Indice: %s" % es_indice
				# print "Type: %s" % es_type
				# print "\n\n\n\n\n\n\n--------------------"



				# Properties (Columns)
				# for es_property, property_value in es_json[es_indice].get('mappings')[es_type].get('properties').iteritems():
				# 	print "column: %s" % es_property
				# 	print property_value

				# Properties data
				# The main reason for this try is because it is ordering basedo on the timestamp field.
				# Ignoring row if timestamp field doesnt exists.
				try:
					es_hits = es.search(index=es_indice, doc_type=es_type, body={"from": 0, "size": LIMIT, "sort": [{"timestamp": {"order": "desc"}}]}).get('hits')
				except elasticsearch.exceptions.RequestError as e:
					#print e					
					continue


				for es_hits_hits in es_hits.get('hits'):
					cs_columns = []
					#print es_hits_hits

					try:
						# Check format for incoming timestamp string.
						try:
							str_timestamp = self.timestamp_converter(es_hits_hits.get('_source').get('timestamp'))
						except AttributeError as e:
							print e
							continue


						quoted_ts = '\''+str(str_timestamp)+'\''
						cs_columns.append(('KEY', 'uuid', es_hits_hits.get('_id')))
						cs_columns.append(('timestamp', 'timestamp', quoted_ts))
						cs_columns.append(('clf_id', 'varchar', '\''+str(es_type)+'\''))

						for eshh in es_hits_hits.get('_source'):
							conv_type, conv_value  = self.type_converter(es_hits_hits.get('_source').get(eshh).__class__.__name__,\
								es_hits_hits.get('_source').get(eshh))

							# This field has already been set	
				 	 		if eshh == 'timestamp' or eshh == 'key' or eshh == 'id':
				 	 			continue

				 	 		# All list fields on Cassandra are created type text.
							if es_hits_hits.get('_source').get(eshh).__class__.__name__ == 'list':
								conv_type = 'list<text>'
								conv_value = []
								for w in es_hits_hits.get('_source').get(eshh):
								 	conv_value.append(str(w))

							cs_columns.append((eshh, conv_type, conv_value))

						cs_insert_kwargs['id'] = es_hits_hits.get('_id')
						cs_insert_kwargs['timestamp'] = str_timestamp
						cs_insert_kwargs['columnfamily'] = es_type
						cs_insert_kwargs['cs_columns'] = cs_columns
						t_checker.check_exists(**cs_insert_kwargs)

					except Exception, e:
						print e
						raise
						pass

	"""
	Change types from Elasticsearch to Cassandra.
	Basically changing unicode to varchar. If more types to convert, just add here.
	If no rule to convert, will return the same.
	"""
	def type_converter(self, entry_type, entry_value):
		if entry_type == 'unicode':
			conv_value = '\'' + entry_value + '\''
			return ('varchar', conv_value)
		else:
			return (entry_type, entry_value)


	"""
	Remove high precision of timestamp for Cassandra.
	Add single cote to string.
	"""
	def timestamp_converter(self, str_timestamp):
		# Remove microseconds
		str_timestamp = str_timestamp.split('.', 1)[0]
		str_timestamp = datetime.strptime(str_timestamp, "%Y-%m-%dT%H:%M:%S")
		return str_timestamp
