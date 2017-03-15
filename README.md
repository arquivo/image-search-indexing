# ImageIndexer
An experimental hadoop image indexer for Web archiving 

```javac -classpath lib/*:/opt/hadoop-1.2.1/hadoop-core-1.2.1.jar: -d .  ImageParse.java ImageSearchResult.java``` 

```javac -classpath lib/*:/opt/hadoop-1.2.1/hadoop-core-1.2.1.jar: -d . IndexImages.java ```

``` /usr/lib/jdk1.7.0_71/bin/jar -cvf /opt/indexV6.jar -C /opt/IndexImages/ .```  

```/opt/hadoop-1.2.1/bin/hadoop jar /opt/indexV6.jar IndexImages /user/root/miniArcs.txt /user/root/output/miniFAWP24v6``` 
