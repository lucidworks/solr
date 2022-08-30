# External Files Module

The External Files Module expands on Solr's core ExternalFileField implementation. 
This module's goal is to create an external file field implementation that scales to larger collections
and supports a larger number of external files, while minimizing the impact of loading and updating of external files
on response time.

The strategy for achieving better performance includes the following:

* Partitioning of external files first by shard and then a second level of partitioning within the shards.
* Sorting the partitioned files by unique id
* Binary formatting of partitioned files to eliminate all parsing and object creation overhead during the load  
* On new and first searchers, inside Solr, sorted partitioned binary files are created that reside in the index 
  with a mapping between unique id and lucene id. Each of these index partitions match up with a partition 
  of the external files.
* Parallel loading of the sorted, binary, partitioned files. The loading is done through a very efficient merge
join between the sorted, partitioned index files and external file partitions. A thread is allocated for
each partition which merges an external partition with its matching internal index partition.  
* Improved caching of internal files to include LRU eviction. This allows external file fields to be loaded and 
  unloaded from memory to support a larger number of external file fields. Because
  external file fields can be loaded quickly the performance hit for reloading an evicted external file is kept
  to a minimum.
* Support for a configurable directory for external files. This allows external files to be mounted as a shared drive
  on each Solr replica to avoid the need to replicate the data to each node.
  
It's useful to contrast this design with the external file field design in Solr core. The design in Solr core
uses a monolithic text file which is optionally sorted. The entire file is loaded in a single thread by 
repeated seeks into the Lucene index to match up the lucene id with a specific float from the file. Sharding 
has limited effect on this because it must still perform seeks for each unique id in the file, and a miss is 
almost as expensive as a hit. There is no LRU cache so all memory can quickly fill up if 
many external file fields are loaded.


# Getting Started

##  Including the Module

The external files module can be included in the Solr startup command like other Solr modules.
The system parameter `EXTERNAL_ROOT_PATH` must also be specified. Below is an example
startup command which includes the external-file module and the `EXTERNAL_ROOT_PATH`:

```bin/solr start -c -m 6g -Dsolr.modules=external-files -DEXTERNAL_ROOT_PATH=$external_file_root```

## ExternalFileUtil (EFU)

The org.apache.solr.util.external.ExternalFileUtil is a command line tool used to process the raw external files
and produce the partitioned output. Syntax:

```
java -cp $solr_modules_root/external-files/build/libs/*:$solr_deployment_root/server/solr-webapp/webapp/WEB-INF/lib/*:$solr_deployment_root/server/lib/\*:$solr_deployment_root/server/lib/ext/* org.apache.solr.util.external.ExternalFileUtil $rawFilesRoot $outRoot $zkHost $collection
```

## ExternalFileListener

The ExternalFileListener is a Solr event listener that creates the partitioned index files, used during
the external file load, after
each new searcher and first searcher. The ExternalFileListener is configured in the solrconfig.xml as
follows:

 ``` 
  <listener event="newSearcher" class="org.apache.solr.util.external.ExternalFileListener">
      
  </listener>
  <listener event="firstSearcher" class="org.apache.solr.util.external.ExternalFileListener">
      
  </listener>
  ```


## ExternalFileField2

The ExternalFileField2 field type is configured in the managed-schema.xml file as follows:

```
<fieldType name="external_float" defVal="0" stored="false" indexed="false" class="org.apache.solr.schema.ExternalFileField2"/>
```

Below is an example of a dynamic field configured to the field type:

```
<dynamicField name="*_ef"  type="external_float"     indexed="false"  stored="false"/>
```

Once configured the `field` function query can be used to access the external float in any part of Solr that
accepts function queries. A sample call would look like this: `field(customer1_ef)`. In this example
the `customer1_ef` name would map to FILENAME directory in the external file root path:

$root/bucket[0-249]/**filename**/timestamp/shardId/partition_[0-7].bin


# How it Works

The External Files Module design can be broken down into four main areas: **partitioning of external files**, 
**index extraction**, **loading of external files** and **caching of external files**.

## Partitioning of External Files

The ExternalFileUtil (EFU) was developed to process the raw external texts into sorted, partitioned, binary
files. Below is a description of the inputs and outputs of the EFU.

### Storage and Format of Raw External Files (Unpartitioned Input)

The raw external files are stored in the following directory structure:

EXTERNAL_FILES_ROOT / N_TOP_LEVEL_SUBDIRS / FILENAME / TIMESTAMP / FILE

#### EXTERNAL FILES ROOT: 

The root directory for the external files.

#### N TOP LEVEL SUB-DIRS:

N Subdirectories to hold files. How many subdirectories, what they are named and which files they hold does not
affect the behavior of the file processing. In the simplest design a single sub-directory can be used. Multiple 
sub-directories can be used if it makes it easier to organize the files.

#### FILENAME

A directory for each file. The directory should be the same as the filename without the extension. 
For example if the file is named **foo.txt** the directory should be named **foo**.

#### TIMESTAMP

The timestamp directories hold different versions of the external files. Timestamp format should be in unix time 
format and parse to a Java long. New versions of the file should be placed in higher timestamps directory.

#### FILE

There should be only 1 enternal file in each timestamp directory. The external file name should end with a .txt extension. Files that are being written should have a tempory extension other
than .txt and be renamed to a .txt extension after being written. If Solr dynamic fields are used the file name 
should end with a "_" postfix that maps directly to a Solr dynamic field. Example file name:

Sample full file path:

$root/bucket1/foo_en/1661540544007/foo_en.txt

The format of the of text file is:

id1:float1
id2:float2

OR

id1:routeKey1:float1
id2:routeKey1:float2

The id must map to a unique Id in a Solr collection. The value must parse to a Java float. The routeKey
must be provided if documents are routed to shards with a specific route key. ExternalFileUtil splits
each file into separate files for each shard. The ids do not need to be sorted as files will be sorted by 
id by the ExternalFileUtil. The (See ExternalFileUtil docs below for details.)


### Storage and Format of Processed External Files (Output)

The raw external files are processed by the ExternalFileUtil (EFU) which is command line tool. 
The EFU creates the following output structure:

$root/bucket[0-249]/filename/timestamp/shardId/partition_[0-7].bin


#### Root

The processed external files root directory.

#### Bucket

250 Sub-directories with the prefix "bucket" and postfix [0-249]. Example directory: bucket1

Filenames are mapped to directories by hashing the filename similar to a hashmap.

#### Filename

Each bucket will contain N filename folders. The folder names will be exactly the same as the filename
directories in the raw files directory structure.

#### timestamp

The unix timestamp also taken from raw files directory structure.

#### shardId

The directories for shards for the collection that external files will be loaded to. For example if the
collection has 5 shards there will 5 shardId folders. Document ids are partitioned by the 
ExternalFileUtil using the DocRouter that is used by the collection. Currently only the hash based
CompositeId router is supported with support for id and shardKey routing.

#### partitions

Within each shardId directory are 8 sorted, binary partition files. Sample file name: partition_0.bin. 
The partitions are created by hashing the id of the document and mapping to one of 8 partitions. 
The record format of the binary files is as follows:

1 byte length of id bytes
N id bytes (the unique id)
4 byte float

## Index Extraction

Before the first searcher and each new searcher are registered an index extraction is done to produce
the sorted, partitioned, binary files which are used to facilitate the high performance load of the external files.
The extracted files are written to the data directory inside of the core they are extracted from in a directory
called *external*. The *external* directory is a sister directory of the *index* directory in the core.

Inside of the external directory are files with following naming convention:

searcher_partition_[0-7]parition_number

Below is sample index extraction file:

6a50778b_partition_0

The binary record format inside of the extract files are:

1 byte length of unique id

N bytes representing the unique id

4 bytes representing a int lucene id


## Loading of the Files

The loading of the processed partitioned files is handled by the new ExternalFileField2 (field type) 
and FileFloatSource2 classes. The external files are loaded lazily upon request of any a schema field 
that is mapped the ExternalFileField2 field type. The FileFloatSource2 class performs the following steps
to load the files:

* Locates the latest timestamp directory for the file name requested. For example if field name 
*customer1_ef* is requested it will locate the latest timestamp inside external file directory tree: 
$root/bucket[0-249]/filename/timestamp

* Locates the shardId directory inside the timestamp directory that matches the core's SolrCloud shardId.

* Using a thread per partition, it merge joins the external partitions with the corresponding index extract partition
and loads the floats into the array.
  
* Once loaded and cached the floats are mapped to lucene ids and can be used anywhere a function query can be used.
This includes: field lists, sorting fields, collapse, facet aggregations, frange filter queries, facet queries (frange filter queries)

## Caching of the Files



 

