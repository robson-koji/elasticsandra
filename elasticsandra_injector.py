from elasticsandra.es_cassandra import CassandraLoader
from elasticsandra.es_elasticsearch import ElasticsearchLoader


import uuid
import time
import random
import string
import elasticsearch
from datetime import datetime



def elasticsearch(qt_indices):
	idx = 0
	while (idx < qt_indices):
		indice =  ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))

		# Create random indices (databases) for Elasticsearch
		random_head="pouiqwer"
		indice = ''.join(random.sample(random_head,len(random_head))) + indice
		es_init_kwargs = {'db': indice}
		injector = ElasticsearchLoader(**es_init_kwargs)


		"""
		Create 5000 documenst and distribute randonly through out 
		the 16 randonly created tables (doc_type).
		"""
		idx_docs = 0
		while (idx_docs < 5000):
			# Sort string randonly
			s="abcd"
			doc_type = ''.join(random.sample(s,len(s)))

			# Create values
			city = "city_"+str(idx_docs)
			firstname = "firstname_"+str(idx_docs)
			lastname = "lastname_"+str(idx_docs)
			email = "email@test_"+str(idx_docs)


			print indice
			print doc_type
			print city
			print firstname
			print lastname
			print email 



			# Fill columns
			es_columns = {'timestamp': datetime.now(),
							'firstname': firstname, 
							'lastname': lastname, 
							'age':  idx, 
							'city': city, 
							'email': email}

			es_insert_kwargs = {'id': str(uuid.uuid4()),
								'doc_type': doc_type,
								'es_columns': es_columns}
			
			idx_docs += 1
			time.sleep(0.01)

			# Inject eache created document
			injector.insert_data(**es_insert_kwargs)

		idx += 1


# Create 20 databases. Increase on your will.
qt_indices = 20
elasticsearch(qt_indices)




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
