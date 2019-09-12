# ImageSearch
An hadoop image indexer for Web archiving - supports ARC/WARC files.

## Algorithm 
### Phase 1 - CreateImageDB
- Iterate through all ARC/WARC records to find all Image records (i.e. records with mimetype that starts with image/)
- Insert those records in a distributed mongodb database.

### Hadoop Phase 2 - IndexImages
- Iterate through all ARC/WARC records to find all HTML records (i.e. records with mimetype that starts with text/html)
- Find all image tags in that html page i.e. ('''<img>'''...'''</img>''').
- For each img tag -> search image in our DB created in step 1. If image found -> add new index for that image to the outputs.
- Remove all images from Phase 1

## Compile
```mvn clean install``` 

## Run
Create a .txt file where each line contains the path to a downloadable ARC/WARC file ($line_ARCS.txt) and store it in Hadoop HDFS


```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar CreateImageDB /user/root/$line_ARCS.txt /user/root/$line_db $line $maxMaps ```

```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar IndexImages /user/root/"$line"_ARCS.txt /user/root/"$line"_db "$line" $maxMaps```  

After hadoop image indexing is finished it is time to export the results

## Export Results
```mongoexport --host $mongoserverlocation:$mongoport --db hadoop_images --collection imageIndexes --out /data/images/$line_uniq.jsonl  --query "{'collection':'$line'}"```  

### Variables
```$line:```  Name of the collection to index (e.g. AWP10) 

```$maxMaps:``` Maximum number of simultaneous running maps (suggested number 50)

``` /user/root/$line_ARCS.txt```  : hdfs path to a file where each line contains the http location of each ARC/WARC file in the collection

``` /user/root/$line_db``` : hdfs output folder

```$mongoserverlocation``` : the ip of the server running mongo / one ip of a Query router in a sharded cluster
```$mongoport``` : the port where the mongo is running in ```$mongoserverlocation```


## Requirements
- Hadoop 3 cluster
- MongoDB server or MongoDB sharded cluster
