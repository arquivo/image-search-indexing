import java.io.IOException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.sun.tools.javadoc.JavaScriptScanner.Reporter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.codec.binary.Base64;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.regex.Pattern;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import java.util.regex.Matcher;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.ArchiveRecord;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;




class ImageMap extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
	private Logger logger = Logger.getLogger(ImageMap.class);
    public static String collectionName;
	private MongoClient mongoClient;
	private DB database;
	private DBCollection MongoCollection;     

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
    	MongoCollection = database.getCollection("images");        
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
    public String md5ofString(String content)  {
    	return DigestUtils.md5Hex(content);
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
    

    public  void createImageDB(ARCRecord record, Context context){
    	System.out.println("creating image DB...");
    	Configuration conf = context.getConfiguration();
    	String url = record.getHeader().getUrl();
    	String tstamp = record.getMetaData().getDate();
    	String mime = record.getMetaData().getMimetype();
    	String collection = conf.get("mapred.job.name");
    	String image_hash_key = md5ofString(url);
    	String content_hash = md5ofString(tstamp+"/"+url);
    	byte[] contentBytes = null;
		try {
			contentBytes = getRecordContentBytes(record);
	    	
	    	DBObject img = new BasicDBObject("_id", new BasicDBObject("image_hash_key", image_hash_key).append("tstamp", tstamp))
                    .append("url", url)
                    .append("tstamp", tstamp)
                    .append("mime", mime)
                    .append("collection", collection)
                    .append("safe", -1)
                    .append("content_hash", content_hash)
                    .append("bytes64string", Base64.encodeBase64String(contentBytes));
	    	MongoCollection.insert(img);
	    	
	    	
//	    	System.out.println("Saving in GRIDFS." );
//	    	GridFS gfsPhoto = new GridFS(database, "gridfsimages" ); /*Create namespace*/ /*collection+"/img/"*/
//	    	GridFSInputFile gfsFile = gfsPhoto.createFile(contentBytes);
//	    	gfsFile.setFilename(content_hash);
//	    	gfsFile.save();
	    	System.out.println("File Inserted: "+content_hash);
	    	
	    	/*write image in hdfs a file with name content_hash*/
		    //FileSystem fs = FileSystem.get(conf);
		    //String s = fs.getHomeDirectory()+"/"+ collection+ "/img/"+ content_hash; 
		    //Path path = new Path(s);
	    	//FSDataOutputStream out = fs.create(path);
		    //out.write(contentBytes);
		    //out.close();

		}catch (IOException e) {
			logger.error("IOException" + e.getMessage() );	
			e.printStackTrace();
		} 
    }

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("FILENAME: " + value.toString());
        ARCReader reader = null;
        try {
            int records = 0;
            int errors = 0;
            
            System.out.println("ARCNAME: " + value.toString());
            reader = ARCReaderFactory.get(value.toString());


            for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
                    ARCRecord record = (ARCRecord)ii.next();
                    if(record.getMetaData().getMimetype().contains("image"))
                        createImageDB(record, context);
                    ++records;
                    if (record.hasErrors()) {
                        errors += record.getErrors().size();
                    }                        
            }
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
            if(mongoClient!=null){
            	mongoClient.close();
            }
        }				
	}
}

class ImageMapReducer extends Reducer<Text, IntWritable, Text,DoubleWritable> {
	public void reduce(Text key, Iterator<IntWritable> values,
	//TODO:: remove reducer in this indexing phase
			OutputCollector<Text, DoubleWritable> output,
			Reporter reporter)
					throws IOException {
		System.out.println("Creating Index for image hash key!");
     	MongoClientOptions.Builder options = MongoClientOptions.builder();
     	options.socketKeepAlive(true);        
     	     	
    	MongoClient mongoClient = new MongoClient( Arrays.asList(
      		   new ServerAddress("p37.arquivo.pt", 27020),
      		   new ServerAddress("p38.arquivo.pt", 27020),
      		   new ServerAddress("p39.arquivo.pt", 27020)), options.build());
    			
    	//DB database = mongoClient.getDB("hadoop_images");
    	//DBCollection mongoCollection = database.getCollection("imageIndexes");
    	//mongoCollection.remove(new BasicDBObject()); /*Remove all documents in imageIndexes*/

    	System.out.println("Created Index");
	}
}



public class CreateImageDB 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    int maxMaps = args.length >=4 ? Integer.parseInt(args[3]) : 50;
    Configuration conf = new Configuration();
    conf.set("collection", args[2]);
    conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
      
	Job job = new Job(conf, "Mapper_Only_Job");
	job.setJarByClass(CreateImageDB.class);
	job.setMapperClass(ImageMap.class);
	job.setJobName(args[2]+"_Images");
	job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
	
    job.setReducerClass(ImageMapReducer.class);
    
	job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, new Path(args[0]));
    
    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
    job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum of 500 simultaneous maps accessing preprod for now*/
  
	// Sets reducer tasks to 1
	job.setNumReduceTasks(1);
	
	boolean result = job.waitForCompletion(true);


	System.exit(result ? 0 : 1);
    }
}
