viadeo-avro-utils
=================


viadeo-avro-utils is a set of mapreduce jobs to perform Data management tasks on avro datasets : 
  - Compact : a simple compaction job, it merge (small) parts into one or more parts.
  - Diff : a record based diff job, take 2 files and output 3 files : add, kernel, del representing respectively records
   that are : only in the first file, in both files, only in the second file.
  - Merge : a record based merge job, take N files and ouput a single one, representing the input with a mask.
  - Extract : extract a source file from a merge file.
  

Build
=======

```
mvn clean package
```
or if you don't have maven : 
```
./mvnw clean package
```


It builds in .... 


Usage
========

Compact
---------



