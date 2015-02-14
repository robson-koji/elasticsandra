

"""
class TheThing(object):
	import datetime

	def __init__(self):
		if checkExists(id):
			if checkNewer():
				self.dt_change = datetime.datetime.now()
				self.db_origin = ''

		else:
			self.id = ''
			self.dt_change = datetime.datetime.now()
			self.db_origin = ''



	objectList = []



	def checkExists(id):
		for obj in objectList:
		    if obj.id == id:
		        return 1
		        break
		else:
		    x = None	


	def checkNewer(id):
		for obj in objectList:
		    if obj.id == id:
		        print "i found it!"
		        break
		else:
		    x = None	




class CommonDataCreator(object):





class ElasticsearchCreator(CommonDataCreator):
    " Classe de Carrier para o relatorio AverageFinancialTermsReportView"
    def __init__(self, o, **kwargs):
        super(AverageFinancialTermsReportViewCarrier, self).__init__( o, **kwargs)
        self.dt_diff = 0        

    def update(self, o, **kwargs):
        super(AverageFinancialTermsReportViewCarrier, self).update( o, **kwargs)
        self.dt_diff += kwargs['dt_diff']

class CassandraCreator(CommonDataCreator):




"""
""" Classe usada para instanciar ou atualizar um objeto """
"""

class TheThingCreator(object):

    def __init__(self, instance_list):        
        self.instance_list = instance_list

    def check_exists(self, *args, **kwargs):
        self.data_object = kwargs['data_object']
        try:
            # Update if exists or create.
            for instance in self.instance_list:
                if instance.object_identifier == kwargs['object_identifier']:
                    instance.update(self.data_object, **kwargs)             
                    return
                else:
                    pass
            instance = kwargs['factory'](self.data_object, **kwargs)                    
            instance.update(self.data_object, **kwargs)             
            self.instance_list.append(instance)
            return

        except Exception, e:
            logger.error('InstanceCreator - check_exists -> %s' %  e)
            pass  



objects_list = []
tt_creator = TheThingCreator(objects_list)


  #       tt_creator = TheThingCreator(objects_list)
  #       data_kwargs['id'] = 
		# data_kwargs['factory'] = ElasticsearchCreator
  #       tt_creator.check_exists(**data_kwargs)

"""



""" Read ElasticSearch """
def readElasticSearch():
	import elasticsearch

	es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

	# Get all Indices (Databases)
	#es_json = es.indices.get_settings(index='_all')
	es_json = es.indices.get_mapping(index='_all')

	# print es_json

	# Get indices
	#print es_json.keys()

	# Indices (Database)
	for es_indice, indice_value in es_json.iteritems():
		print "Indice: %s" % es_indice
		# print indice_value

		# Types (Tables)
		for es_type, type_value in es_json[es_indice].get('mappings').iteritems():
			print "Type: %s" % es_type
			# print type_value

			# Properties (Columns)
			for es_property, property_value in es_json[es_indice].get('mappings')[es_type].get('properties').iteritems():
				print "column: %s" % es_property
				print property_value

			# Properties data
			es_hits = es.search(index=es_indice, doc_type=es_type).get('hits')
			# for es_hits_hits in es_hits.get('hits'):
			# 	print es_hits_hits
				# try:
				# 	print "_id: %s" % es_hits_hits.get('_id')
				# 	print "timestamp: %s" % es_hits_hits.get('_source').get('timestamp')
				# except Exception, e:
				# 	print e
				# 	pass






""" Read Cassandra """
def readCassandra():

	#
	## Check how does Python connects to Cassandra, and credential
	#

	from cassandra.cluster import Cluster
	cluster = Cluster()
	session = cluster.connect('demo')

	
	""" Get keyspaces (Databases) """
	keyspaces = session.execute(" SELECT keyspace_name FROM system.schema_keyspaces ")

	for k in keyspaces:
		keyspace = k.keyspace_name

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
			 	#print c.type
				#print "column: %s" % c.column_name

			""" Get field values (Columns)"""
			values = session.execute(" SELECT * FROM " + columnfamily )

			for v in values:
			 	for vf in v._fields:
			# 		print type(vf)
					print type(getattr(v, vf))
					print getattr(v, vf)





		print "\n\n"







def load_elasticsearch():
	import uuid
	import elasticsearch
	from datetime import datetime

	es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

	es.index(index='posts', doc_type='blog', id=str(uuid.uuid4()), body={
	    'author': 'Santa Clause',
	    'blog': 'Slave Based Shippers of the North',
	    'title': 'Using Celery for distributing gift dispatch',
	    'topics': ['slave labor', 'elves', 'python',
	               'celery', 'antigravity reindeer'],
	    'awesomeness': 0.2,
	    "timestamp": datetime.now()
	})	
	es.index(index='posts', doc_type='blog', id=str(uuid.uuid4()), body={
	    'author': 'Benjamin Pollack',
	    'blog': 'bitquabit',
	    'title': 'Having Fun: Python and Elasticsearch',
	    'topics': ['elasticsearch', 'python', 'parseltongue'],
	    'awesomeness': 0.7,
	    "timestamp": datetime.now()
	})
	es.index(index='posts', doc_type='blog', id=str(uuid.uuid4()), body={
	    'author': 'Benjamin Pollack',
	    'blog': 'bitquabit',
	    'title': 'How to Write Clickbait Titles About Git Being Awful Compared to Mercurial',
	    'topics': ['mercurial', 'git', 'flamewars', 'hidden messages'],
	    'awesomeness': 0.95,
	    "timestamp": datetime.now()
	})



def load_cassandra():
	import cassandra
	from cassandra.cluster import Cluster
	cluster = Cluster()
	session = cluster.connect('demo')
	# session = cluster.connect()

	import uuid

	"""
		Indice: movies
		Type: movie
		column: director
		column: genres
		column: year
		column: title
		Indice: posts
		Type: blog
		column: awesomeness
		column: title
		column: timestamp
		column: topics
		column: author
		column: blog
		Indice: asdf
		Type: blog
		column: blog
		column: title
		column: topics
		column: awesomeness
		column: author
	"""


	def create_keyspace(keyspace):
		try:		
			session.execute(
				"""
				CREATE KEYSPACE posts WITH 
				replication = {'class':'SimpleStrategy', 'replication_factor':3}; 		     
				"""
				)
		except cassandra.AlreadyExists as e:
			print e
	
	#values = session.execute(" SELECT * FROM " + columnfamily )


	def create_columnfamily(columnfamily):
		try:		
			session.execute(
				"""
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
			 	"""
			 	)
		except cassandra.AlreadyExists as e:
			print e
	
	#exit(0)


	# session = cluster.connect('posts')


# {u'_score': 1.0, u'_type': u'blog', u'_id': u'0b350530-5395-4fb6-820a-349903ae857c', u'_source': {u'awesomeness': 0.2, u'author': u'Santa Clause', u'timestamp': u'2015-02-13T12:36:28.589949', 
# u'topics': [u'slave labor', u'elves', u'python', u'celery', u'antigravity reindeer'], u'title': u'Using Celery for distributing gift dispatch', u'blog': u'Slave Based Shippers of the North'}, u'_index': u'posts'}

#insert into movie (id, timestamp, awesomeness, author, topics, title, blog) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'Ana')


#			values = session.execute(" SELECT * FROM " + columnfamily )

	def insert_data(columnfamily):
		import re
		try:
			session.execute("insert into" + columnfamily + "(lastname, age, city, email, firstname) \
				values ('Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'asdf')")
		except cassandra.InvalidRequest as e:
			# Error code and msg of the exception returns one long string only. 
			# To catch the specific exception, I had to use this regex.
			# Need to fix and send a patch to the driver team to return exceptions correct.
			if re.search('unconfigured columnfamily' , e.message) is not None:


				print "------------"
				print repr(e)
				print e.message
				print e.args[0]
				print "------------"

				create_columnfamily(columnfamily)

				session.execute("""
			 		insert into asdf (lastname, age, city, email, firstname) values ('Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'asdf')		 
				 """)


	insert_data(asdf)




def asdf():
	session.execute("""
	 insert into users (id, timestamp, lastname, age, city, email, firstname) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome1', 30, 'Sao Paulo', 'ana@example.com', 'Ana')
	 """)


	session.execute("""
	 insert into users (id, timestamp, lastname, age, city, email, firstname) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome2', 31, 'Sao Paulo', 'joao@example.com', 'Joao')
	 """)
	session.execute("""
	 insert into users (id, timestamp, lastname, age, city, email, firstname) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome3', 32, 'Rio de Janeiro', 'paulo@example.com', 'Paulo')
	 """)
	session.execute("""
	 insert into users (id, timestamp, lastname, age, city, email, firstname) values (str(uuid.uuid4()), datetime.now(), 'Sobrenome4', 33, 'Curitiba', 'maria@example.com', 'Maria')
	 """)




	session.execute(
	     """
	     INSERT INTO users (id, timestamp, lastname, age, city, email, firstname)
	     VALUES (%(id)s, %(timestamp)s, %(lastname)s, %(age)s, %(city)s, %(email)s, %(firstname)s)
	    {'id': uuid.uuid4(), 'timestamp': datetime.now(), 'lastname': "Sobrenome1", 'age': 30, 'city': "Sao Paulo", 'email': "ana@example.com", 'firstname': "Ana"}
	     """
	)

	session.execute(
	     """
	     INSERT INTO users (id, timestamp, lastname, age, city, email, firstname)
	     VALUES (%(id)s, %(timestamp)s, %(lastname)s, %(age)s, %(city)s, %(email)s, %(firstname)s)
	    {'id': uuid.uuid4(), 'timestamp': datetime.now(), 'lastname': "Sobrenome2", 'age': 31, 'city': "Sao Paulo", 'email': "joao@example.com", 'firstname': "Joao"}
	     """
	)

	session.execute(
	     """
	     INSERT INTO users (id, timestamp, lastname, age, city, email, firstname)
	     VALUES (%(id)s, %(timestamp)s, %(lastname)s, %(age)s, %(city)s, %(email)s, %(firstname)s)
	    {'id': uuid.uuid4(), 'timestamp': datetime.now(), 'lastname': "Sobrenome3", 'age': 32, 'city': "Rio de Janeiro", 'email': "paulo@example.com", 'firstname': "Paulo"}
	     """
	)

	session.execute(
	     """
	     INSERT INTO users (id, timestamp, lastname, age, city, email, firstname)
	     VALUES (%(id)s, %(timestamp)s, %(lastname)s, %(age)s, %(city)s, %(email)s, %(firstname)s)
	    {'id': uuid.uuid4(), 'timestamp': datetime.now(), 'lastname': "Sobrenome4", 'age': 33, 'city': "Curitiba", 'email': "maria@example.com", 'firstname': "Maria"}
	     """
	)





readCassandra()
#readElasticSearch()
#load_elasticsearch()
#load_cassandra()

#exit(0)


# prepared_stmt = session.prepare ( "SELECT * FROM users WHERE (lastname = ?)")
# bound_stmt = prepared_stmt.bind(['Sobrenome1'])
# stmt = session.execute(bound_stmt)
# for x in stmt: print x.firstname, x.age



# result = session.execute("select * from users where lastname='Jones' ")[0]
# print result.firstname, result.age
