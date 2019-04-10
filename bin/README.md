# ImageSearch
An experimental hadoop image indexer for Web archiving 

```javac -classpath lib/*:/opt/hadoop-1.2.1/hadoop-core-1.2.1.jar: -d .  ImageParse.java ImageSearchResult.java``` 

```javac -classpath lib/*:/opt/hadoop-1.2.1/hadoop-core-1.2.1.jar: -d . IndexImages.java ```

``` /usr/lib/jdk1.7.0_71/bin/jar -cvf /opt/indexImages.jar -C /github/ImageSearch/ .```  

```/opt/hadoop-1.2.1/bin/hadoop jar /opt/indexImages.jar IndexImages /user/root/FAWP10.txt /user/root/output/FAWP10``` 
