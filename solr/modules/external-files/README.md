# External Files Module

The External Files Module expands on Solr's core ExternalFileField implementation. 
This module's goal is to create an external file field implementation that scales to larger collections
and supports a larger number of external files, while minimizing the impact of loading and updating of external files
on response time.

The strategy for achieving better performance are the following:

* Partitioning of external files first by shard and then a second level of partitioning within the shards.
* Sorting the partitioned files by unique id
* Binary formatting of partitioned files to eliminate all parsing and object creation overhead during the load  
* On new and first searchers, inside Solr, sorted partitioned binary files are created that reside in the index 
  with a mapping between unique id and lucene id. Each of these index partitions match up with a partition 
  of the external files.
* Parallel loading of the sorted, binary, partitioned files. The loading is done through a very efficient merge
join between the sorted, partitioned index files and external file partitions. A thread is allocated for
each partition which merges an external partition with its matching internal index partition.  
* Improved caching of internal files to support LRU eviction. This allows external file fields to be loaded and 
  unloaded from memory to support a larger number of external file fields than can fit in memory at once. Because
  external file fields can be loaded quickly the performance hit for reloading an evicted external file is kept
  to a minimum.
* Support for a configurable directory for external files. This allows external files to be mounted as a shared drive
  on each Solr replica to avoid the need to replicate the data to each node.
  
It's useful to contrast this design with the external file field design in Solr core. The design in Solr core
uses a monolithic text file which is optionally sorted. The entire file is loaded in a single thread by 
repeated seeks into the Lucene index to match up the lucene id with a specific float from the file. Sharding 
has limited effect on this because it must still perform seeks for each unique id in the file and a miss is 
almost as expensive as a hit. There is no LRU cache so all of memory can quickly fill up if 
a large number of external file fields are loaded.
  

# Design

The External Files Module design can be broken down into four main areas: **handling of external files**, 
**handling of the index**, **loading of external files** and **caching of external files**.

## Handling of External Files


### Storage and Format of Raw External Files (Input)

The raw external files will stored in the following directory structure:

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

The process external file root directory.

#### Bucket

250 Sub-directories with the prefix "bucket" and postfix [0-249]. Example directory: bucket1

Filenames are mapped to directories by hashing the filename similar to a 250 bucket hashmap.

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




## Handling of Index


## Loading of the Files


## Caching of the Files


## Software Components
 

### ExternalFileUtil


### ExternalFileListener


### ExternalFileField2 and FileFloatSource2

# Setup