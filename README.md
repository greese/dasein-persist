dasein-persist
==============

The Dasein Persistence libraries manage the persistence of Java objects to backend data stores such as relational and
NoSQL databases. Dasein Persistence will automatically map a Java object to the backend data store and handle in-JVM
caching of objects to minimize communication with the database. The caching scheme differs from something like memcache
in that it is not distributed. This enables a solid balance between memory management within an individual JVM and
freshness of data.

Dasein Persistence supports:

* Any JDBC backend
* Special high performance MySQL backends
* Riak
