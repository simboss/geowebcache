.. _concepts.stores:

Storage components
==================

GeoWebCache historically used to have three storage components, responsible for both tile and tile metadata handling: the blob store, the metastore and the disk quota subsystem.

The **blobstore** is a storage mechanism for tiles, whose default implementation is file system based.

The **metastore** was an optional H2 based storage mechanism for meta-information about tiles, such as tile creation time, size and usage of request parameters.

The **disk quota** mechanism uses a nosql embedded database to track the tiles disk usage and expire tiles based on user set policies.

Since GeoWebCache 1.4.0 the metastore was replaced with a full filesystem based solution, making the blobstore responsible for the information previously tracked by the metastore.

By default, the storage location for both of these stores is the temporary storage directory specified by the servlet container (a directory called :file:`geowebcache` will be created there.). If this directory is not available, GeoWebCache will attempt to create a new directory in the location specified by the ``TEMP`` environment variable.  Inside there will be one directory for the disk quota (called :file:`diskquota_page_store` by default), and blobstore directories named after each cached layer (such as :file:`topp_states` for the layer ``topp:states``).

You can change the default **blobstore** implementation with a new one called **MemoryBlobStore**, which allows in memory tile caching. For changing the **blobstore** used, you have to 
modify the **blobstore** bean associated to the **gwcStorageBroker** bean (inside the file *geowebcache-core-context.xml*) by setting *gwcMemoryBlobStore* instead of *gwcBlobStore* . In the same file you can
also setup a new configuration for the cache object used by the MemoryBlobStore. The parameters you can set are:

	* *hardMemoryLimit* : which is the cache size in Mb
	* *policy* : which can be LRU, LFU, EXPIRE_AFTER_WRITE, EXPIRE_AFTER_ACCESS, NULL 
	* *evitionTime* : which is the cache eviction time in seconds
	* *concurrencyLevel* : which is the cache concurrency level
	
These parameters must be defined as properties in the **cacheConfiguration** bean in the same XML file.
