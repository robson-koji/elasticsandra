#README


##About Elasticsandra
This is a program that have been developed to maintain two systems in sync, for instance Elasticsearch and Cassandra.

I developed this program while learning both technologies and it is just a POC, not to be used in production environment. 

It was implemented in a point of view of RDBs and not NOSql persistence systems, and some access to databases are made by navigating through the schema, instead of direct data access. This turns the system slow in some cases.

Cassandra requires that in order to navigate through its schema and top use some traditional SQL like ORDER BY, LIMIT etc, we have to create a compose Primary Key with the fields that are to be sorted, limited etc. Thanks to some other limitations that I was imposed, not to have compose PKs, it is not possible to use some standards SQL statements as cited above. So working with large database can be onerous. I tried with about 200.000 rows in each database, and it runs fine, but it takes long time to read the whole structure.

Some filters have to be implemented direct on the databases layer too, to put pressure on databases engine instead of on the application layer.  



##Download and install
It is recommend that you use virtualenv and create an environment to install Elasticsandra.

Git clone the repository in your home, or elsewhere. A directory 'elastisandra' will be created on git clone.

- cd ~ 
- git clone https://github.com/fortinbras/elasticsandra.git
- pip install elasticsandra/dist/elasticsandra-0.1.tar.gz

Install required dependencies to finalize de installation process.



##Libs
Once you have finished the installation process, you will have had installed Elasticsandra libs.

These libs will be used by the daemon and by the injector script, documented on the following sections.

Source of this libs are in:
*<installation_dir>/elasticsandra/elasticsandra/elasticsandra.py*
*<installation_dir>/elasticsandra/elasticsandra/es_cassandra.py*
*<installation_dir>/elasticsandra/elasticsandra/es_elasticsearch.py*



##Injector script
*<installation_dir>/elasticsandra/elasticsandra_injector.py*

This script creates random data to load in Elasticsearch and Cassandra.

If you want to start a test from the scratch, cleanup Elasticsearch and Cassandra and run this script.

It will create 2 DBs for Elasticsearch and 2 DBs for Cassandra, 16 tables for each DB with 7 colums for each table and 500 rows for each table.

This test creates some random data. Not to much entropy but it is enough for test.

If you already have Elasticsearch and/or Cassandra DBs, you can try to use Elasticsandra with them. But you may face unkown bugs.

**To run the injector:**
*python elasticsandra_injector.py*

You can run this injector as many as you want, and you can change internal parameters to inject more data, create new columns etc.

You can run this injector while the daemon is running, to see data replication between Elastisearch and Cassandra and vice versa.



##Start Elasticsearch and Cassandra
Be sure that Cassandra and Elasticsearch are running, or you will have errors.
*sudo service cassandra start*
*sudo service elasticsearch start*

Elasticsandra has been tested with Elasticsearch and Cassandra on default ports configuration, and no access control for both. If you have custom values you may face problems. No different parameters configurations were tested.



##Daemon
*<installation_dir>/elasticsandra/elasticsandra.py*

It runs as a daemon and starts as ordinary daemons and receives an additional parameter to control interval for repetition.
*elasticsandra.py start <interval in seconds>*

It reads data from Elasticsearch and Cassandra, check if data is synchronized or synchronize and store in an internal dictionary.


##Known bugs
The first time the daemon runs, if data exists in databases it assumes that data databases data are in sync. If this is not true, on the first round old data can be replicated instead of new data from one database to another. 