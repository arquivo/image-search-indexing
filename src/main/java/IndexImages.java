import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kerby.util.Base64;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.ArchiveRecord;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;
import com.sun.tools.javadoc.JavaScriptScanner.Reporter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.log4j.Logger;


public class IndexImages 
{
public static class Map extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		 private Logger logger = Logger.getLogger(Map.class);
		 public static String collectionName;
		 private MongoClient mongoClient;
	  	 private DB database;
	  	 private DBCollection collection;
	  	 private DBCollection imageIndexes;

	    @Override
	    public void setup(Context context) {
	        Configuration config = context.getConfiguration();
	        collectionName = config.get("collection");
	        System.out.println("collection: " + collectionName);
	     	MongoClientOptions.Builder options = MongoClientOptions.builder();
	     	options.socketKeepAlive(true);
	    	MongoClient mongoClient = new MongoClient( Arrays.asList(
	       		   new ServerAddress("p37.arquivo.pt", 27020),
	       		   new ServerAddress("p38.arquivo.pt", 27020),
	       		   new ServerAddress("p39.arquivo.pt", 27020)), options.build());
	     	database = mongoClient.getDB("hadoop_images");
	     	collection = database.getCollection("images");	
	     	imageIndexes = database.getCollection("imageIndexes");
	    	System.out.println(collectionName+"_Images"+"/img/");
	    }
	    
		private byte[] getRecordContentBytes(ARCRecord record) throws IOException {
	    	record.skipHttpHeader();/*Skipping http headers to only get the content bytes*/
	    	byte[] buffer = new byte[1024 * 16];    	
	    	int len = record.read(buffer, 0, buffer.length);
	        ByteArrayOutputStream contentBuffer =
	        		new ByteArrayOutputStream(1024 * 16* 1000); /*Max record size: 16Mb*/               
	        contentBuffer.reset();
	        while (len != -1)
	        {
	          contentBuffer.write(buffer, 0, len);
	          len = record.read(buffer, 0, buffer.length);
	        }
	        record.close();      
	        return contentBuffer.toByteArray();
		}         

		public static String guessEncoding(byte[] bytes) {
		    String DEFAULT_ENCODING = "UTF-8";
		    org.mozilla.universalchardet.UniversalDetector detector =
		        new org.mozilla.universalchardet.UniversalDetector(null);
		    detector.handleData(bytes, 0, bytes.length);
		    detector.dataEnd();
		    String encoding = detector.getDetectedCharset();
		    detector.reset();
		    if (encoding == null) {
		        encoding = DEFAULT_ENCODING;
		    }
		    return encoding;
		}

	    private static final Pattern VALID_PATTERN = Pattern.compile("[0-9A-Za-z]*");

	    private String parseURL(String toParse) {
	        String result = "";
	        Matcher matcher = VALID_PATTERN.matcher(toParse);
	        while (matcher.find()) {
	            result+= matcher.group() + " ";
	        }
	        return result;
	    }
	    
	    private JSONArray stringToJsonArray(String content){
	    	JSONArray result = new JSONArray();
	    	String[] tokens = content.split("\\s+");
	    	for(String current: tokens){
	    		result.add(current);
	    	}
	    	return result;
	    }


	    public  void parseImagesFromHtmlRecord(ARCRecord record, Context context){
	        try{
	        	System.out.println("Parsing Images from ARCrecord");
	        	logger.info("Parsing Images from ARCrecord" );
	            byte[] arcRecordBytes = getRecordContentBytes(record);
	            logger.info("Read Content Bytes from ARCrecord" );
	            System.out.println("Read Content Bytes from ARCrecord" );
	            String recordEncoding = guessEncoding(arcRecordBytes); 
				InputStream is = new ByteArrayInputStream(arcRecordBytes);  

	            Document doc = Jsoup.parse(is, recordEncoding, "");
	            String pageTitle = doc.title(); /*returns empty string if no title in html document*/

	            Elements imgs = doc.getElementsByTag("img");
	            int pageImages = imgs.size();
	            String pageURL = record.getHeader().getUrl();
	            //String pageURLDigest = ImageParse.hash256(pageURL);
	            String pageURLCleaned = URLDecoder.decode(pageURL, "UTF-8"); /*Escape URL e.g %C3*/
	            pageURLCleaned = StringUtils.stripAccents(pageURLCleaned); /* Remove accents*/
	            String pageURLTokens = parseURL(pageURLCleaned); /*split the URL*/


	            URL uri = new URL(pageURL);
	            String pageHost = uri.getHost();
	            String pageProtocol = uri.getProtocol();   
	            String pageTstamp = record.getMetaData().getDate();
	            if (pageTstamp == null || pageTstamp.equals("")){
	                System.err.println("Null pageTstamp");                
	            }
	            System.out.println("pageTstamp:" + pageTstamp);
	            

	            for(Element el: imgs){
	                JSONObject obj = new JSONObject();      
	                String imgSrc = el.attr("src");

	                if(!imgSrc.startsWith("http") && !imgSrc.startsWith("data:image")){
	                    /*Relative Path lets reconstruct full path*/
	                    /*TODO:: check how to reconstruc relative paths*/
	                    if(imgSrc.startsWith("/")){ /* Relative path starting in the host*/
	                            imgSrc = pageProtocol+ "://"+ pageHost + imgSrc;
	                    }
	                    else{
	                        imgSrc= pageURL + "/" + imgSrc;
	                    }
	                }
	                if(imgSrc.length() > 10000 || pageURL.length() > 10000){
	                    System.err.println("URL of image too big ");
	                    System.err.println(pageURL.substring(0,500) + "...");
	                    continue;
	                }/*Maximum size for SOLR index is 10 000*/                
	                if (imgSrc == null || imgSrc.equals("")){
	                    System.out.println("Null imgSrc");
	                    continue;
	                }
	                ImageSQLDTO imgSQLDTO = retreiveImageFromDB(DigestUtils.md5Hex(imgSrc), Long.parseLong(pageTstamp), context.getConfiguration());
	                if(imgSQLDTO == null){
	                	System.err.println("Got null image for hash: "+ DigestUtils.md5Hex(imgSrc));
	                	continue;
	                }
	                ImageSearchResult imgResult = ImageParse.getPropImage(imgSQLDTO);
	                if ( imgResult == null ){
	                    logger.error("Failed to create thumbnail for image: "+ imgSrc + "with ts: "+imgSQLDTO.getTstamp());
	                    continue;
	                }

	               
	                
	                
	                String imgSrcCleaned = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
	                imgSrcCleaned = StringUtils.stripAccents(imgSrcCleaned); /* Remove accents*/
	                String imgSrcTokens = parseURL(imgSrcCleaned); /*split the imgSrc URL*/                
	                
	                String imgTitle = el.attr("title");
	                if(imgTitle.length() > 9999){imgTitle =  imgTitle.substring(0, 10000); }
	                String imgAlt = el.attr("alt");
	                if(imgAlt.length() > 9999){imgAlt =  imgAlt.substring(0, 10000); }                

	                
	                
	                insertImageIndexes(imgResult, imgSrc,imgSQLDTO, imgSrcTokens,imgTitle, imgAlt, pageImages, pageTstamp, pageURL, pageHost, pageProtocol,  pageTitle, pageURLTokens);
	                
	                logger.info("Written to file - successfully indexed image record" );
	                System.out.println("Written to file - successfully indexed image record" );
	            }
	        } catch (Exception e){
	        	logger.error("ERROR..." + e.getMessage());
	        	System.err.println("Something failed JSOUP parsing");
	        	System.out.println("Something failed JSOUP parsing");       	
	        	e.printStackTrace();
	        }

	    }


		/* Retreives ImageSQLDTO with the closest date to pageTstamp
	     * Returns null if no results found in the SQL DB
	     * 
	     */
	    public ImageSQLDTO retreiveImageFromDB(String imgHashKey, long pageTstamp, Configuration conf) {
	    	BasicDBObject whereQuery = new BasicDBObject();
	  	  	whereQuery.put("_id.image_hash_key", imgHashKey);
	  	  	DBCursor cursor = collection.find(whereQuery);

	        long tstampDiff = 99999999999999L; /*Difference between pageStamp and imgTstamp, we want the closest possible*/
	        long imgTstamp = -1;
	        long currentTstamp = -1;
	        String mime ="";
	        String imgContentHash = null;
	        byte[] retrievedImgBytes = null;
	  	  	  	  	
	  	  	while (cursor.hasNext()) {
	  	  		DBObject currentResult = cursor.next();
	  	  		String ctstamp =(String) currentResult.get("tstamp");
	  	  		if(ctstamp == null) {
	  	  			System.err.println("Empty tstamp for image with hash: " + imgHashKey);
	  	  			return null;
	  	  		};
	        	currentTstamp = Long.parseLong(ctstamp);
	    		if(Math.abs(currentTstamp - pageTstamp) < tstampDiff){
	    			imgTstamp = currentTstamp;
	    			imgContentHash = (String)  currentResult.get("content_hash");
	    			mime = (String) currentResult.get("mime");
	    			tstampDiff = currentTstamp - pageTstamp;
	    			retrievedImgBytes = Base64.decodeBase64((String) currentResult.get("bytes64string"));
	    			if(tstampDiff == 0){break;} /*No need to continue the cycle if the imgtstamp is the same as the pagetstamp*/
	    		}  	  		
	  	  	}
	        
				return imgTstamp == -1 ?  null : new ImageSQLDTO(retrievedImgBytes, mime, String.valueOf(imgTstamp));
	        
	    }


	    public void insertImageIndexes(ImageSearchResult imgResult, String imgSrc, ImageSQLDTO imgSQLDTO,
				String imgSrcTokens, String imgTitle, String imgAlt, int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol, String pageTitle, String pageURLTokens) throws NoSuchAlgorithmException {
	    	String imgDigest = imgResult.getDigest();
	    	String imgTstamp = imgSQLDTO.getTstamp();
	    	Long imgTstampLong = Long.parseLong(imgTstamp);
	    	DBObject img;
	    	
	    	BasicDBObject whereQuery = new BasicDBObject();
	  	  	whereQuery.put("imgDigest", imgDigest);
	  	  	DBCursor cursor = imageIndexes.find(whereQuery);
	  	  	int numberofImagesWithHashKey =cursor.size();
	  	  	if(numberofImagesWithHashKey <=1){ /*Create object to insert or update the DB*/
	  			img = new BasicDBObject()
	  				.append("_id", imgResult.getDigest())
	  				.append("imgDigest", imgResult.getDigest()) /*Intentionally repeated field*/ 
	    			.append("imgTitle", imgTitle)
	    			.append("imgAlt", imgAlt)
	    			.append("imgWidth", imgResult.getWidth())
	    			.append("imgHeight", imgResult.getHeight())
	                .append("safe", -1)
	  	    	    .append( "imgSrc", imgSrc) /*The URL of the Image*/
	        		.append( "imgTstamp", imgSQLDTO.getTstamp())
	        		.append( "imgSrcTokens", imgSrcTokens)
	        		.append( "imgSrcURLDigest", ImageParse.hash256(imgSrc)) /*Digest Sha-256 of the URL of the Image*/            
	        		.append( "imgMimeType" ,  imgResult.getMime( ) )
	        		.append( "imgSrcBase64" , imgResult.getThumbnail( ) )
	        		.append( "imgDigest" , imgResult.getDigest( ) )
	        		.append("pageImages", pageImages)
	        		.append( "pageTstamp" , pageTstamp )
	        		.append( "pageURL" , pageURL ) /*The URL of the Archived page*/
	        		.append( "pageHost", pageHost)
	        		.append( "pageProtocol", pageProtocol)
	        		.append( "pageTitle" , pageTitle) /*The URL of the Archived page*/
	        		.append("pageURLTokens", pageURLTokens)                
	        		.append( "collection" , collectionName );  
	  	  	
		  	  	if(numberofImagesWithHashKey == 0){ /*insert image it is unique in our imageIndexes collection*/                  
			    	imageIndexes.insert(img);
			    	System.out.println("inserted:" + imgResult.getDigest());
			    	System.out.println("inserted ts:" + imgSQLDTO.getTstamp());		    	
		  	  	}
		  	  	else if (numberofImagesWithHashKey == 1){ /*check if this image tstamp is more old than the one we currently store if it is update this record in the db*/
		  	  		DBObject currentResult = cursor.next(); /*get the record*/ 
		  	  		String dataBaseTstamp =(String) currentResult.get("imgTstamp");  	  		
		  	  		if(dataBaseTstamp == null) {
		  	  			System.err.println("Empty tstamp for image with hash: " + imgDigest);
		  	  			return;
		  	  		}
		  	  		Long dBTstamp = Long.parseLong(dataBaseTstamp);
		  	  		if(imgTstampLong <= dBTstamp){ /*Found an older version of this image digest lets update the database*/
		  	  			imageIndexes.update(whereQuery, img);
				    	System.out.println("updated: " + imgResult.getDigest());
				    	System.out.println("updated TS: " + imgSQLDTO.getTstamp());	  	  			
		  	  		}
		  	  		else{ /*There is an older version of this digest in our db so we won't insert this one*/
		  	  			return;
		  	  		} 	  		
		  	  	}
	  	  	}  	
	  	  	else{
	  	  		/*This should never happen because imgDigest is unique in our DB*/
	  	  		System.err.println("Multiple records with hash key: " + imgDigest);
	  	  	}
	    }    
	    
	    
	    
	    /*public byte[] readImgContentFromGridFS(String imgContentHash, Configuration conf) throws IOException{
	    	GridFSDBFile imageForOutput = gfsPhoto.findOne(imgContentHash);
	    	System.out.println("ImageHash: " + imgContentHash);
	    	System.out.println(imageForOutput);
	    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    	imageForOutput.writeTo(baos);
	    	return baos.toByteArray();    	    	
	    }*/
	    
	    
	    public byte[] readImgContentFromHDFS(String imgContentHash, Configuration conf) throws IOException{
	    	/*write image in hdfs a file with name content_hash*/
	    	String collection = conf.get("mapred.job.name");
		    FileSystem fs = FileSystem.get(conf);
		    String s = fs.getHomeDirectory()+"/"+ collection+ "/img/"+ imgContentHash; 
		    Path path = new Path(s);
	    	FSDataInputStream in = fs.open(path);
	        byte[] imgContent = new byte[in.available()];
	        in.read(imgContent);
		    in.close();
		    return imgContent;    	
	    }


		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			logger.info("Map Started for ARCNAME: " + value.toString());
	        ARCReader reader = null;
	        try {
	            int records = 0;
	            int errors = 0;
	            
	            System.out.println("ARCNAME: " + value.toString());
	            
	            
	            logger.info("Started Reading ARCNAME: " + value.toString());
	            reader = ARCReaderFactory.get(value.toString());
	            logger.info("Ended Reading ARCNAME: " + value.toString());

	            for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
	            	 	logger.info("Reading record number " + records+1);
	                    ARCRecord record = (ARCRecord)ii.next();
	                    if(record.getMetaData().getMimetype().contains("html"))
	                        parseImagesFromHtmlRecord(record, context);
	                    ++records;
	                    if (record.hasErrors()) {
	                        errors += record.getErrors().size();
	                    }                        
	            }
	            System.out.println("--------------");
	            System.out.println("       Records: " + records);
	            System.out.println("        Errors: " + errors);
	            
	            context.write(new LongWritable(0L), NullWritable.get()); //dumb code write 0 , NULLWritable to send to the reducer phase
	            
	        }
	        catch (FileNotFoundException e) {
	            // TODO Auto-generated catch block
	            System.err.println("ARCNAME: " + value.toString());
	            e.printStackTrace();
	        }
	        catch (IOException e) {
	            // TODO Auto-generated catch block
	            System.err.println("ARCNAME: " + value.toString());
	            e.printStackTrace();
	        }
			catch(Exception e){
			    System.err.println("Unhandled exception?");
			    e.printStackTrace();
			}
	        finally{
	            if(reader!=null){
	                reader.close();
	            }
	            if(mongoClient != null){
	            	mongoClient.close(); /*Close connection to MongoDB*/
	            }
	        }				
		}
	}	
	
public static class IndexReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
	private Logger logger = Logger.getLogger(IndexReducer.class);
	
	 public static String collectionName;
	 private MongoClient mongoClient;
 	 private DB database;
 	 private DBCollection images;
 	 private DBCollection imageIndexes;	
	
	
    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        collectionName = config.get("collection");
        System.out.println("collection: " + collectionName);
     	MongoClientOptions.Builder options = MongoClientOptions.builder();
     	options.socketKeepAlive(true);
    	MongoClient mongoClient = new MongoClient( Arrays.asList(
       		   new ServerAddress("p37.arquivo.pt", 27020),
       		   new ServerAddress("p38.arquivo.pt", 27020),
       		   new ServerAddress("p39.arquivo.pt", 27020)), options.build());
     	database = mongoClient.getDB("hadoop_images");
     	images = database.getCollection("images");	
    }	
	
    public void reduce(LongWritable key, Iterable<NullWritable> values,
            Context context
            ) throws IOException, InterruptedException {
    	System.out.println("Reduce IndexImages");
    	BasicDBObject query = new BasicDBObject();
    	query.append("collection", collectionName+"_Images");		
    	images.remove(query);
    	
    	context.write(new LongWritable(0L), NullWritable.get()); //dumb code write 0 , NULLWritable to finish reduce phase
    	
	}
}		

public static void retryShardCollection(int numberOfRetries, int numberOfSecondsTimeout, MongoClient mongoClient, BasicDBObject cmd, Logger logger ){
	CommandResult result=null; 
	for(int i=0; i<numberOfRetries;i++){
		try{
			TimeUnit.MINUTES.sleep(2); //sleep 2 minutes before attempting to shard collection again
		}catch (InterruptedException e){
            logger.info("Interrupted while Sleeping"); 
        } 
		result = mongoClient.getDB("admin").command(cmd);
		if (result.ok()){
			logger.info("Successfully recreated images db");
			return;
		}		
	}
	logger.error("Error sharding collection images: "+ result.getErrorMessage());
}


public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
	int maxMaps = args.length >=4 ? Integer.parseInt(args[3]) : 40;    	
    Configuration conf = new Configuration();
    conf.set("collection", args[2]);

    Job job = Job.getInstance(conf, "Index Images");

	job.setJarByClass(IndexImages.class);
	job.setMapperClass(Map.class);
	
	job.setMapOutputValueClass(NullWritable.class);
	job.setOutputFormatClass(NullOutputFormat.class);
	

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(NullWritable.class);
	
	job.setReducerClass(IndexReducer.class);
	
	job.setJobName(args[2]+"_Images");
	

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, new Path(args[0]));
    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
    job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum o simultaneous maps accessing preprod for now*/
    
		
	// Sets reducer tasks to 1
	job.setNumReduceTasks(1);

	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean result = job.waitForCompletion(true);

	System.exit(result ? 0 : 1);
    }
}
