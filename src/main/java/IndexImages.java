import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLDecoder;
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.log4j.Logger;



class Map extends Mapper<LongWritable, Text, Text, Text> {
	 private Logger logger = Logger.getLogger(Map.class);
	 public static String collectionName;
	 private MongoClient mongoClient;
  	 private DB database;
  	 private DBCollection collection;
  	 private GridFS gfsPhoto;

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        collectionName = config.get("collection");
        System.out.println("collection: " + collectionName);
     	MongoClientOptions.Builder options = MongoClientOptions.builder();
     	options.socketKeepAlive(true);
    	MongoClient mongoClient = new MongoClient( Arrays.asList(
        		   new ServerAddress("p12.arquivo.pt", 27020),
        		   new ServerAddress("p39.arquivo.pt", 27020)), options.build());
     	database = mongoClient.getDB("hadoop_images");
     	collection = database.getCollection("images");	  
    	gfsPhoto = new GridFS(database,collectionName+"_Images/img/");
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
        	logger.error("Parsing Images from ARCrecord" );
            byte[] arcRecordBytes = getRecordContentBytes(record);
            logger.info("Read Content Bytes from ARCrecord" );
            logger.error("Read Content Bytes from ARCrecord" );
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

                obj.put( "imgWidth", imgResult.getWidth( ) ); 
                obj.put( "imgHeight", imgResult.getHeight( ) ); 
                obj.put( "imgSrc", imgSrc); /*The URL of the Image*/
                obj.put( "imgTstamp", imgSQLDTO.getTstamp());
                obj.put( "imgSrcTokens", imgSrcTokens);
                obj.put( "imgSrcURLDigest", ImageParse.hash256(imgSrc)); /*Digest Sha-256 of the URL of the Image*/
                
                
                if(el.attr("title").length() > 9999){
                    obj.put( "imgTitle", el.attr("title").substring(0,10000));
                }
                else{
                    obj.put( "imgTitle", el.attr("title"));
                }
                if(el.attr("alt").length() > 9999){
                    obj.put( "imgAlt", el.attr("alt").substring(0,10000));
                }
                else{
                    obj.put( "imgAlt", el.attr("alt"));
                }
                obj.put( "imgMimeType" ,  imgResult.getMime( ) );
                obj.put( "imgSrcBase64" , imgResult.getThumbnail( ) );
                obj.put( "imgDigest" , imgResult.getDigest( ) );

                obj.put("pageImages", pageImages);
                obj.put( "pageTstamp" , pageTstamp );
                obj.put( "pageURL" , pageURL ); /*The URL of the Archived page*/
                obj.put( "pageHost", pageHost);
                obj.put( "pageProtocol", pageProtocol);
                if(! pageTitle.isEmpty()){
                    obj.put( "pageTitle" , pageTitle); /*The URL of the Archived page*/
                }
                obj.put("pageURLTokens", pageURLTokens);                
                obj.put( "collection" , collectionName );
                                
                context.write( new Text (obj.toJSONString()),null);
                
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
    			if(tstampDiff == 0){break;} /*No need to continue the cycle if the imgtstamp is the same as the pagetstamp*/
    		}  	  		
  	  	}
        
        try {
			return imgTstamp == -1 ?  null : new ImageSQLDTO(readImgContentFromGridFS(imgContentHash, conf), mime, String.valueOf(imgTstamp));
		} catch (IOException e) {
			System.err.println("Error retrievImageFromDB");
			e.printStackTrace();
			return null;
		}
        
    }

    public byte[] readImgContentFromGridFS(String imgContentHash, Configuration conf) throws IOException{
    	GridFSDBFile imageForOutput = gfsPhoto.findOne(imgContentHash);
    	System.out.println("ImageHash: " + imgContentHash);
    	System.out.println(imageForOutput);
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	imageForOutput.writeTo(baos);
    	return baos.toByteArray();    	    	
    }
    
    
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


public class IndexImages 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
	int maxMaps = args.length >=4 ? Integer.parseInt(args[3]) : 40;    	
    Configuration conf = new Configuration();
    conf.set("collection", args[2]);

	Job job = new Job(conf, "Mapper_Only_Job");

	job.setJarByClass(IndexImages.class);
	job.setMapperClass(Map.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJobName(args[2]+"_Images");
	

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, new Path(args[0]));
    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
    job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum o simultaneous maps accessing preprod for now*/
    

	job.setOutputFormatClass(TextOutputFormat.class);
		
	// Sets reducer tasks to 0
	job.setNumReduceTasks(0);

	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean result = job.waitForCompletion(true);

	System.exit(result ? 0 : 1);
    }
}
