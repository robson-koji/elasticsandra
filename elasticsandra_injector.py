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
#qt_indices = 20
#elasticsearch(qt_indices)




import cassandra
import re



def cassandra(qt_indices):
	idx = 0
	while (idx < qt_indices):
		keyspace =  ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(20))

		# Create random indices (databases) for Elasticsearch
		random_head="pouiqwer"
		keyspace = ''.join(random.sample(random_head,len(random_head))) + keyspace
		cs_init_kwargs = {'db': keyspace}
		injector = CassandraLoader(**cs_init_kwargs)

		"""
		Create 5000 documenst and distribute randonly through out 
		the 16 randonly created tables (columnfamily).
		"""
		idx_docs = 0
		while (idx_docs < 5000):			# Sort string randonly
			s="abcd"
			columnfamily = ''.join(random.sample(s,len(s)))

			# Create values
			city = '\''+"city_"+str(idx_docs)+'\''
			firstname = '\''+"firstname_"+str(idx_docs)+'\''
			lastname = '\''+"lastname_"+str(idx_docs)+'\''
			email = '\''+"email@test_"+str(idx_docs)+'\''

			print keyspace
			print columnfamily
			print city
			print firstname
			print lastname
			print email 

			str_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
			quoted_ts = '\''+str_timestamp+'\''

			cs_columns = []
			cs_insert_kwargs = {}

			cs_columns.append(('KEY', 'uuid', str(uuid.uuid4())))
			cs_columns.append(('timestamp', 'timestamp', quoted_ts))
			cs_columns.append(('clf_id', 'varchar', '\''+str(columnfamily)+'\''))

			cs_columns.append(('firstname', 'varchar', firstname))
			cs_columns.append(('lastname', 'varchar', lastname))
			cs_columns.append(('age', 'int', idx))			
			cs_columns.append(('city', 'varchar', city))						
			cs_columns.append(('email', 'varchar', email))

			cs_insert_kwargs['id'] = str(uuid.uuid4())
			cs_insert_kwargs['timestamp'] = str(datetime.now())
			cs_insert_kwargs['columnfamily'] = columnfamily
			cs_insert_kwargs['cs_columns'] = cs_columns
			
			idx_docs += 1
			time.sleep(0.01)

			# Inject eache created document
			injector.insert_data(**cs_insert_kwargs)

		idx += 1




# Create 20 databases. Increase on your will.
qt_indices = 1
cassandra(qt_indices)


