# ImageSearch
An experimental hadoop image indexer for Web archiving 

## Compile
```mvn clean install``` 

## Run

```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar CreateImageDB /user/root/"$line"_ARCS.txt /user/root/"$line"_db "$line" 125 ```

```hadoop jar ImageSearch-1.0-SNAPSHOT-jar-with-dependencies.jar IndexImages /user/root/"$line"_ARCS.txt /user/root/"$line"_db "$line" 90```  

