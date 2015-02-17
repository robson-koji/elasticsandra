from cassandra.cluster import Cluster
from datetime import datetime

import cassandra
import uuid
import re

cluster = Cluster()

"""
Provides methods to handle Cassandra schema.
"""
class CassandraSchemaHandler(object):
	def __init__(self, *args, **kwargs):
		self.keyspace = kwargs['db']
		#self.session = kwargs['session']


	def create_keyspace(self):
		self.session = cluster.connect()
		try:		
			self.session.execute(" CREATE KEYSPACE " +  self.keyspace + " WITH replication = {\'class':'SimpleStrategy', 'replication_factor':3}; ")
		except cassandra.AlreadyExists as e:
			raise
			exit(0)


	def create_columnfamily(self, *args, **kwargs):
		self.columnfamily = kwargs['columnfamily']
		self.columns = kwargs['cs_columns']		
		self.session = kwargs['session']		


		# This composition of Primary Keys is necessary to allow ordering.
		# Ordering is needed for slice while selecting and limiting qt of returning query.
		# clf_id creates an artificial table to allow ordering. Whithout EQ or IN it is not
		# possible to order, so the select is equal his own columnfamilly.
		creating_columns = "( " + ",".join("{0} {1}".format(cl[0], cl[1]) for cl in self.columns) + \
								", PRIMARY KEY (clf_id, timestamp, KEY)) WITH CLUSTERING ORDER BY (timestamp DESC)"
		


		print "================"
 		print "creating columnfamily: " + str(self.columnfamily)
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
		self.keyspace = kwargs['db']
		self.cs_sch_hdl = CassandraSchemaHandler(**kwargs)
	
		try:
			self.session = cluster.connect(self.keyspace)
		except cassandra.InvalidRequest as e:
			# Error code and msg of the exception returns one long string only. 
			# To catch the specific exception, I had to use regex.			
			#if re.search('does not exist' , e.message) is not None:
				self.cs_sch_hdl.create_keyspace()
				self.session = cluster.connect(self.keyspace)

	def insert_data(self, *args, **kwargs):
		self.columnfamily = kwargs['columnfamily']
		self.columns = kwargs['cs_columns']
		kwargs['session'] = self.session

		self.cl_keys = " (" + ",".join("%s" % cl[0] for cl in self.columns)  + ") "
		self.cl_values = " (" + ",".join("%s" % cl[2] for cl in self.columns)  + ") "

		print self.cl_keys
		print self.cl_values
		# exit(0)

		self.try_insert(**kwargs)


	def try_insert(self, *args, **kwargs):

		try:
			self.session.execute("insert into " + self.columnfamily + self.cl_keys + " values " + self.cl_values)
			print "++++++++++++>OK!"
			return 

		except cassandra.InvalidRequest as e:
			# Error code and msg of the exception returns one long string only. 
			# To catch the specific exception, I had to use regex.
			if re.search('unconfigured columnfamily' , e.message) is not None:
				self.cs_sch_hdl.create_columnfamily(**kwargs)

				# Recursive call to try again.
				self.try_insert(**kwargs)
			elif re.search('unable to coerce' , e.message) is not None:
				print e

			elif re.search('for key of type uuid' , e.message) is not None:
				print e
			else:
				print e
			print "aqui"				
			print "------------>NOK!"
		except cassandra.protocol.SyntaxException as e:
			print "ou aqui"			
			print self.columnfamily
			print self.cl_keys
			print self.cl_values
			print "------------>NOK!"
			print e


				


# Constant to limit number of register to synchronize each round.
# Delay for each round is defined on daemon initialization.
LIMIT = 1000


import time

class CassandraReader(object):
	def __init__(self, objects_dict):

		self.objects_dict = objects_dict
		# keyspaces = self.session.execute(" SELECT keyspace_name FROM system.schema_keyspaces")		
		# print "asdfasdfsdf"
		# print keyspaces
 		# print "\n\n\nINIT OK\n\n\n"

	def read_cassandra(self):
		from elasticsandra import TheChecker
		self.session = cluster.connect()

		""" Get keyspaces (Databases) """
		keyspaces = self.session.execute(" SELECT keyspace_name FROM system.schema_keyspaces ")
		#keyspaces = self.session.execute(" SELECT keyspace_name FROM system.schema_keyspaces")		

		print keyspaces

		# Arguments to send to TheChecker
		tc_kwargs = {'objects_dict': self.objects_dict, 'caller': CassandraLoader} 

		for k in keyspaces:
			start = time.time()

			keyspace = k.keyspace_name

			# if keyspace != "ahz3gng779mmzm1cnb1h":
			# 	continue

			self.session = cluster.connect(keyspace)

			# Instantiate TheCheker for each keyspace
			tc_kwargs['db'] = keyspace
			t_checker = TheChecker(**tc_kwargs)
			# Cassandra will models output data specific to Elasticsearch and vice versa.
			es_insert_kwargs = {}

			# Ignore system tables
			if keyspace == 'system' or keyspace == 'system_traces':
				continue

			print "keyspace: %s" % keyspace

			""" Get columnfamilies (Tables) """
			prepared_stmt = self.session.prepare( "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ? ")
			bound_stmt = prepared_stmt.bind(k)
			columnfamilies = self.session.execute(bound_stmt)

			for cf in columnfamilies:
				columnfamily = cf.columnfamily_name

				# if columnfamily != "ab12":
				# 	continue


				print "columnfamilies: %s" % columnfamily

				""" Get columns (Columns)"""
				prepared_stmt = self.session.prepare(" SELECT * FROM system.schema_columns WHERE keyspace_name = ? AND columnfamily_name = ? ")
				bound_stmt = prepared_stmt.bind([keyspace, columnfamily])
				columns = self.session.execute(bound_stmt)

				# for c in columns:
				# 	print dir(c)
				#  	print c.type
				# 	print "column: %s" % c.column_name

				""" Get field values (Columns)"""
				try:			
					query = " SELECT * FROM " + columnfamily + " WHERE clf_id = \'" +columnfamily+ "\' ORDER BY timestamp DESC LIMIT " + str(LIMIT)
					rows = self.session.execute(query)

					es_columns = {}

					for row in rows:
						# print row
						try:
							es_insert_kwargs['doc_type'] = columnfamily
							es_insert_kwargs['timestamp'] = row.timestamp
							es_insert_kwargs['last_change'] = row.last_change
							es_insert_kwargs['id'] = row.key

							# Filling es_columns dict inside try, to avoid id or timestamp errors
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
				 	 		print e
				 	 		pass

				except cassandra.InvalidRequest, e:
					#print e
					pass

			print "\n\n"


			# (or do something more productive)
			done = time.time()

			if done - start < 2:
				time.sleep(2)
