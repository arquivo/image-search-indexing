# ImageSearch
An experimental hadoop image indexer for Web archiving - supports ARC/WARC files.

## Compile
```mvn clean install``` 

## Run

```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar CreateImageDB /user/root/$line_ARCS.txt /user/root/$line_db $line 125 ```

```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar IndexImages /user/root/$line_ARCS.txt /user/root/$line_db $line 90```  

After hadoop image indexing is finished it is time to export the results

## Export Results
```mongoexport --host $mongoserverlocation:$mongoport --db hadoop_images --collection imageIndexes --out /data/images/$line_uniq.jsonl  --query "{'collection':'$line'}"```  

### Variables
```$line:```  Name of the collection to index (e.g. AWP10) 

``` /user/root/$line_ARCS.txt```  : hdfs path to a file where each line contains the http location of each ARC/WARC file in the collection

``` /user/root/$line_db``` : hdfs output folder

```$mongoserverlocation``` : the ip of the server running mongo / one ip of a Query router in a sharded cluster
```$mongoport``` : the port where the mongo is running in ```$mongoserverlocation```


## Requirements
- Hadoop 3 cluster
- MongoDB server or MongoDB sharded cluster
