# External Files Module

The External Files Module expands upon Solr's cores ExternalFileField implementation. 
This module's goal is to create an external file field implementation that scales to larger collections
and supports a larger number of external files, while minimizing the impact of loading and updating of external files
on response time.

# Design

The External Files Module design can be broken down into four main areas: **handing oexternal files**, 
**handling of the index**, **loading of external files** and **caching of external files**.

## Handling of External Files


### Storage and Format of Raw External Files (Input)

The raw external files will stored in the following directory structure:

**EXTERNAL_FILES_ROOT** / **N_TOP_LEVEL_SUBDIRS** / **FILENAME** / **TIMESTAMP** / **FILE**

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

key1:value1
key2:value2

The key must map to unique Id in Solr collection. The value must parse to a Java float.
The keys do not need to be sorted as files will be sorted by key by the ExternalFileUtil (see below)


### Storage and Format of Process External Files (Output)





## Handling of Index


## Loading of the Files


## Caching of the Files


## Software Components
 

### ExternalFileUtil


### ExternalFileListener


### ExternalFileField2 and FileFloatSource2

# Setup