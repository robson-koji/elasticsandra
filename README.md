#README


##About Elasticsandra
This is a program that have been developed to maintain two systems in sync, for instance Elasticsearch and Cassandra. It checks both DBs and sync the other. 

There is a script running in daemon mode, that call the libs of the system to do this. 

It is expected that Elasticsandra creates the whole structure on both sides, mirroring the one it reads. Talking in RDBs language, it creates the DB, tables, columns and acctual data (rows).

I developed this program while learning both technologies and it is just a POC, not to be used in production environment. 

It was implemented in a point of view of RDBs and not NoSQL persistence systems, and some access to databases are made by navigating through the schema, instead of direct data access. This turns the system slow in some cases.

Cassandra requires that in order to navigate through its schema and to use some traditional SQL statements like ORDER BY, LIMIT etc, we have to create a composed Primary Key with the fields that are to be sorted, limited etc. Thanks to some other limitations that I was imposed, not to have compose PKs, it is not possible to use some standards SQL statements as cited above. So working with large database can be onerous. I tried with about 200.000 rows in each database, and it runs fine, but it takes some time to read the whole structure.

Some filters have to be implemented direct on the databases layer too, to put pressure on databases engine instead of the application layer.  



##Download and install
It is recommended that you use virtualenv and create an environment to install Elasticsandra.

Git clone the repository in your home, or elsewhere. A directory called *elastisandra* will be created after git clone.

- cd ~ 
- git clone https://github.com/fortinbras/elasticsandra.git
- pip install elasticsandra/dist/elasticsandra-0.1.tar.gz

Install required dependencies to finalize the installation process.



##Libs
Once you have finished the installation process, you will have had installed Elasticsandra libs.

These libs will be used by the daemon and by the injector script, which are documented bellow.

Elasticsandra libs were installed on your virtuaenv python libs dir (assuming you are using virtualenv). These are the source code path:

- *installation_dir/elasticsandra/elasticsandra/elasticsandra.py*
- *installation_dir/elasticsandra/elasticsandra/es_cassandra.py*
- *installation_dir/elasticsandra/elasticsandra/es_elasticsearch.py*



##Injector script
*installation_dir/elasticsandra/elasticsandra_injector.py*

This script creates random data to load in Elasticsearch and Cassandra.

If you want to start a test from the scratch, cleanup first Elasticsearch and Cassandra and run this script to load some initial data. 

It will create 2 DBs for Elasticsearch and 2 DBs for Cassandra, 16 tables for each DB with 7 colums for each table, and 500 rows for each table.

This test creates some random data. Not to much entropy but it is enough for test.

If you already have Elasticsearch and/or Cassandra DBs, you can try to use Elasticsandra with them. But you may face unkown bugs.

**To run the injector:**

*python elasticsandra_injector.py*

You can run this injector as many times you want, and you can change internal parameters to inject more data, create new columns etc.

You can run this injector while the daemon is running, to see data replication between Elastisearch and Cassandra and vice versa.



##Start Elasticsearch and Cassandra
Be sure that Cassandra and Elasticsearch are running, or you will have errors.

*sudo service cassandra start*

*sudo service elasticsearch start*

Elasticsandra has been tested with Elasticsearch and Cassandra on default ports configuration, and no access control for both. If you have custom values you may face problems. No different configuration parameters were tested.


##Daemon
*installation_dir/elasticsandra/elasticsandra.py*

Elasticsandra daemon runs and starts as ordinary daemons and receives an additional parameter to control interval for repetition.

** python elasticsandra/elasticsandra.py start (interval in seconds)**

If you get some erros like this:

*ImportError: No module named cassandra.cluster*

Install dependencies and everything will gonna be right.




It reads data from Elasticsearch and Cassandra, check if data is synchronized or synchronize. After that it creates an internal indice in a dictionary to control synchronization.
If you run Elasticsandra daemon with empty Elasticsearch and/or empty Cassandra, the daemon will raise an error due to empty structure. Inject data first.

If you want to see ongoing information on your console, change output from /dev/null to /dev/tty for stdout and sterr on the daemon code. It is all there.


##Update test
To test synchronization of new data, you can use the injector or any other GUI, frontend, curl etc.

To test update of existing data, you can use a GUI for Cassandra and ES or curl for ES and update anything you want, as long as you update the field timestamp. 

**Timestamp field is always checked to verify which register (row, document) is newer, and update de older.**

If a new column (schema changing) is created on Elasticsearch, it will not be replicated to Cassandra. Consider this as a constrain of the Elasticsandra, or a new feature to be developed.



##Known bugs
- The first time the daemon runs, if data exists in databases it assumes that databases are in sync. If this is not true, on the first round, old data can be replicated instead of new data from one database to another. 
- Contents inside of list type are all translated to type text in Cassandra. 
- Timestamp data is comming as unicode on the Python/Elasticsearch driver, instead of timestamp type. Maybe a driver bug!?
- **There is a maing BUG in Cassandra which annoyed me during development, because Cassandra dyes and the cause is not well documented. I could not investigate in depth, but I think that it relates to this one: https://datastax-oss.atlassian.net/browse/PYTHON-124**



##Enhancements
- Make direct access to data on Cassandra and Elasticsearch, instead of schema navigation. 
- Perform bulk insertion instead of atomic transactions.
- Are there lazy load transactions on Cassandra and/or Elasticsearch?
- Create a configuration file to Elasticsandra read properties like ports, urls, users, passwords etc.
- Check all data types conversion.
- Document the API.



##Development environment
Elasticsandra relies on Elasticsearch and Cassandra driver for Python, besides Elasticsearch and Cassandra their own, of course.

###Python drivers:
- cassandra-driver==2.1.4
- elasticsearch==1.4.0

###Programs:
- cassandra-2.0.11
- elasticsearch-1.3.1
- lucene4.9
- elasticsandra==0.1

###Others:
- python-daemon==1.6.1
- time-uuid==0.1.1
- Ubuntu 64
- Linux 3.13.0-45
