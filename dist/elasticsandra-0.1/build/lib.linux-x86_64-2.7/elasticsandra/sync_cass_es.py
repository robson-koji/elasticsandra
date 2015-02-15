

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





from cassandra.cluster import Cluster
from datetime import datetime

import cassandra
import uuid
import re


"""
Provides methods to handle Cassandra schema.
"""
class CassandraSchemaHandler(object):
	def __init__(self, *args, **kwargs):
		self.cluster = Cluster()
		self.keyspace = kwargs['db']
		self.session = kwargs['session']


	def create_keyspace(self):
		self.session = self.cluster.connect()
		try:		
			self.session.execute(" CREATE KEYSPACE " +  self.keyspace + " WITH replication = {\'class':'SimpleStrategy', 'replication_factor':3}; ")
		except cassandra.AlreadyExists as e:
			raise
			exit(0)


	def create_columnfamily(self, *args, **kwargs):
		self.columnfamily = kwargs['columnfamily']
		self.columns = kwargs['columns']		

		creating_columns = "( " + ",".join("{0} {1} {2}".format(cl[0], cl[1], cl[3]) for cl in self.columns)  + ") "

		print "================"
		print creating_columns
		print "================"

		try:		
			self.session.execute("CREATE COLUMNFAMILY " +  self.columnfamily + creating_columns)
			"""			
			self.session.execute(

				CREATE COLUMNFAMILY columnfamily(
					KEY varchar PRIMARY KEY,
					id uuid,
					timestamp timestamp,

					awesomeness varchar,
					author varchar,
					topics list<text>,
					title varchar,
					blog varchar,
					)
			 	)
			"""
		except cassandra.AlreadyExists as e:
			print e



class CassandraLoader(object):
	"""
	Receives kwargs to initialize the object.
	If keyspace does not exist, create on init.
	"""
	def __init__(self, *args, **kwargs):
		self.cluster = Cluster()
		self.keyspace = kwargs['db']
	
		try:
			self.session = self.cluster.connect(self.keyspace)
		except cassandra.InvalidRequest as e:
			# Error code and msg of the exception returns one long string only. 
			# To catch the specific exception, I had to use regex.			
			#if re.search('does not exist' , e.message) is not None:
				self.cs_sch_hdl.create_keyspace()
				self.session = self.cluster.connect(self.keyspace)

		kwargs['session'] = self.session
		self.cs_sch_hdl = CassandraSchemaHandler(**kwargs)

	
	#exit(0)
	# session = cluster.connect('posts')
# {u'_score': 1.0, u'_type': u'blog', u'_id': u'0b350530-5395-4fb6-820a-349903ae857c', u'_source': {u'awesomeness': 0.2, u'author': u'Santa Clause', u'timestamp': u'2015-02-13T12:36:28.589949', 
# u'topics': [u'slave labor', u'elves', u'python', u'celery', u'antigravity reindeer'], u'title': u'Using Celery for distributing gift dispatch', u'blog': u'Slave Based Shippers of the North'}, u'_index': u'posts'}
#insert into movie (id, timestamp, awesomeness, author, topics, title, blog) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'Ana')
#			values = session.execute(" SELECT * FROM " + columnfamily )

	def insert_data(self, *args, **kwargs):
		self.columnfamily = kwargs['columnfamily']
		self.columns = kwargs['columns']

		cl_keys = " (" + ",".join("%s" % cl[0] for cl in self.columns)  + ") "
		cl_values = " (" + ",".join("%s" % cl[2] for cl in self.columns)  + ") "

		print cl_keys
		print cl_values
		#exit(0)

		try:
			#self.session.execute("insert into " + columnfamily + "(lastname, age, city, email, firstname) \
			#	values ('Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'asdf')")

			self.session.execute("insert into " + self.columnfamily + cl_keys + " values " + cl_values)


		except cassandra.InvalidRequest as e:
			# Error code and msg of the exception returns one long string only. 
			# To catch the specific exception, I had to use regex.
			#if re.search('unconfigured columnfamily' , e.message) is not None:

				# print "------------"
				# print repr(e)
				# print e.message
				# print e.args[0]
				# print "------------"

				self.cs_sch_hdl.create_columnfamily(**kwargs)
				self.session.execute("insert into " + self.columnfamily + cl_keys + " values " + cl_values)

				# session.execute("""
			 		# insert into asdf (lastname, age, city, email, firstname) values ('Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'asdf')		 
				#  """)

cs_columns = [('KEY', 'uuid', str(uuid.uuid4()), 'PRIMARY KEY'),
				('timestamp', 'timestamp', '\'2015-02-14 17:48:38\'', ''), 
				('firstname', 'varchar', '\'Jojo\'', ''), 
				('lastname', 'varchar', '\'Sobrenome xyz\'', ''), 
				('age', 'int', 8, ''), 
				('city', 'varchar', '\'Maracatu\'', ''), 
				('email', 'varchar', '\'jojo@jojo.com\'', '')]

cs_init_kwargs = {'keyspace': 'xyz'}
cs_insert_kwargs = {'columnfamily':'bbb',
					'columns': cs_columns}






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
        
        # Working only while working with two dbs,
        # Elasticsearch and Cassandra.
        for not_caller in data_origin_list:
        	if not_caller != self.caller:
				# Instantiate the DB to update.
				self.updater = not_caller(**not_caller_kwargs)

    def check_exists(self, *args, **kwargs):
    	self.id = kwargs['id'] 
    	self.date = kwargs['timestamp'] 



    	if self.id not in objects_dict or (self.id in objects_dict and self.date > objects_dict[self.id]):
			# Update DB
			self.updater.insert_data(**kwargs)

			# Update dict item with actual date
			objects_dict[self.id] = self.date



objects_dict = {}

""" Read ElasticSearch """
def readElasticSearch():
	import elasticsearch

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

# readElasticSearch()
# exit(0)


# cs_columns = [('KEY', 'uuid', str(uuid.uuid4()), 'PRIMARY KEY'),
# 				('timestamp', 'timestamp', '\'2015-02-14 17:48:38\'', ''), 
# 				('firstname', 'varchar', '\'Jojo\'', ''), 
# 				('lastname', 'varchar', '\'Sobrenome xyz\'', ''), 
# 				('age', 'int', 8, ''), 
# 				('city', 'varchar', '\'Maracatu\'', ''), 
# 				('email', 'varchar', '\'jojo@jojo.com\'', '')]

# cs_init_kwargs = {'keyspace': 'xyz'}
# cs_insert_kwargs = {'columnfamily':'bbb',
# 					'columns': cs_columns}




""" Read Cassandra """
def readCassandra():

	#
	## Check how does Python connects to Cassandra, and credential
	#
	import cassandra
	from cassandra.cluster import Cluster
	cluster = Cluster()
	session = cluster.connect()

	
	""" Get keyspaces (Databases) """
	keyspaces = session.execute(" SELECT keyspace_name FROM system.schema_keyspaces ")

	# Arguments to send TheChecker
	tc_kwargs = {'objects_dict': objects_dict, 'caller': CassandraLoader} 

	for k in keyspaces:
		keyspace = k.keyspace_name
		session = cluster.connect(keyspace)

		# Instantiate TheCheker for each keyspace
		tc_kwargs['db'] = keyspace
		t_checker = TheChecker(**tc_kwargs)
		# Cassandra will models output data specific to Elasticsearch
		# and vice versa.
		es_insert_kwargs = {}

		# Ignore system tables
		if keyspace == 'system' or keyspace == 'system_traces':
			continue

		print "keyspace: %s" % keyspace

		""" Get columnfamilies (Tables) """
		prepared_stmt = session.prepare( "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ? ")
		bound_stmt = prepared_stmt.bind(k)
		columnfamilies = session.execute(bound_stmt)

		for cf in columnfamilies:
			columnfamily = cf.columnfamily_name
			print "columnfamilies: %s" % columnfamily

			""" Get columns (Columns)"""
			prepared_stmt = session.prepare(" SELECT * FROM system.schema_columns WHERE keyspace_name = ? AND columnfamily_name = ? ")
			bound_stmt = prepared_stmt.bind([keyspace, columnfamily])
			columns = session.execute(bound_stmt)

			# for c in columns:
			# 	print dir(c)
			#  	print c.type
			# 	print "column: %s" % c.column_name

			""" Get field values (Columns)"""
			try:			
				rows = session.execute(" SELECT * FROM " + columnfamily )
				es_columns = {}

				for row in rows:
					# print row
					try:
						es_insert_kwargs['doc_type'] = columnfamily
						es_insert_kwargs['timestamp'] = row.timestamp
						es_insert_kwargs['id'] = row.id

			 	 		# print row.timestamp
			 	 		# print row.id

						# Filling es_columns dict inside try, to avoid
						# id or timestamp errors
				 	 	for column in row._fields:
				 	 		# ID uuid field went outside data kwargs.
				 	 		# Remove here or will get wrong.
				 	 		if type(getattr(row, column)) is uuid.UUID:
				 	 			continue

				 	 		es_columns[column] = getattr(row, column)
							# print type(getattr(row, column)).__name__

						es_insert_kwargs['es_columns'] = es_columns
						t_checker.check_exists(**es_insert_kwargs)

			 	 	except AttributeError, e:
			 	 		# print e
			 	 		pass
		

			except cassandra.InvalidRequest, e:
				#print e
				pass


		print "\n\n"


readCassandra()
exit(0)

