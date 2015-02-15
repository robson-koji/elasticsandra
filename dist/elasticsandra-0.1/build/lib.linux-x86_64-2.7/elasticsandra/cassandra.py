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



