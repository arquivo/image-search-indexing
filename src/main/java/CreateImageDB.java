import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import  org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.json.simple.JSONArray;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.sun.tools.javadoc.JavaScriptScanner.Reporter;

/* 
 * LOGs where changed to DEBUG level due to Lack of space in hadoop machines 
 * */


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


	public  void createImageDB(WARCRecordResponseEncapsulated record, Context context){
		Configuration conf = context.getConfiguration();
		String url = record.getWARCRecord().getHeader().getUrl();
		String tstamp = record.getTs();
		String mime = record.getContentMimetype();

		String collection = conf.get("mapred.job.name");
		String image_hash_key = md5ofString(url);
		String content_hash = md5ofString(tstamp+"/"+url);
		byte[] contentBytes = null;
		contentBytes = record.getContentBytes();

		DBObject img = new BasicDBObject("_id", new BasicDBObject("image_hash_key", image_hash_key).append("tstamp", tstamp))
				.append("url", url)
				.append("tstamp", tstamp)
				.append("mime", mime)
				.append("collection", collection)
				.append("safe", -1)
				.append("content_hash", content_hash)
				.append("bytes64string", Base64.encodeBase64String(contentBytes));
		MongoCollection.insert(img);
	}
	public  void createImageDB(ARCRecord record, Context context){
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

		}catch (IOException e) {
			logger.debug("IOException" + e.getMessage() );
		} 
	}


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			System.out.println("(W)ARCNAME: " + value.toString());
			logger.info("(W)ARCNAME: " + value.toString());            
			if(value.toString().endsWith("warc.gz") || value.toString().endsWith("warc")){
				readWarcRecords(value.toString(), context);
			}else{
				readArcRecords(value.toString(), context);
			}
		}catch(Exception e){
			logger.debug("Error Reading ARC/WARC" + e.getMessage());
		}finally{
			if(mongoClient!=null){
				mongoClient.close();
			}
		}	

	}

	private void readArcRecords(String arcURL, Context context) {
		int records= 0;
		int errors = 0;
		ArchiveReader reader = null;
		try{				
			reader = ARCReaderFactory.get(arcURL);
			for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
				 try{
					ARCRecord record = (ARCRecord)ii.next();
					if(record.getMetaData().getMimetype().contains("image"))
						createImageDB(record, context);
					++records;
					if (record.hasErrors()) {
						errors += record.getErrors().size();
					}
				 }catch(Exception e){
					 logger.debug("exception reading ARC record" + e.getMessage());
				 }
			}
		}catch (FileNotFoundException e) {
			logger.debug("ARCNAME: " + arcURL + " "+e.getMessage());
		}
		catch (IOException e) {
			logger.debug("ARCNAME: " + arcURL+ " "+e.getMessage());
		}
		catch(Exception e){
			logger.debug("Unhandled exception? "+ e.getMessage());
		} finally{
			logger.debug("records: " + records);
			logger.debug("errors: " + errors);
			if(reader!=null){
				try {
					reader.close();
				} catch (IOException e) {
					logger.debug("error closing ArchiveReader"+ e.getMessage());
				}
			}
		}

	}


	private void readWarcRecords(String warcURL, Context context) {
		logger.debug("Reading WARC records");
		int records= 0;
		int errors = 0;
		ArchiveReader reader = null;
		try{				
			reader = WARCReaderFactory.get(warcURL);
			for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
				try{
					WARCRecordResponseEncapsulated record =new WARCRecordResponseEncapsulated((WARCRecord) ii.next());
					logger.debug("WARC Response");
					if(record.getContentMimetype().contains("image")){ /*only processing images*/
						logger.debug("Found image in WARC record");
						createImageDB(record, context);			
					}
					++records;
					if (record.hasErrors()) {
						errors += record.getErrors().size();
					}
				}catch(InvalidWARCResponseIOException e){
					/*This is not a WARCResponse; skip*/	
				}
				catch(IOException e){
					logger.debug("IO Exception reading WARCrecord WARCNAME: " + warcURL+ " " + e.getMessage());				
				}catch (Exception e){
					logger.debug("Exception reading WARCrecord WARCNAME: " + warcURL+ " " + e.getMessage());
				}
			}
		}catch (FileNotFoundException e) {
			logger.debug("WARCNAME: " + warcURL+ " " + e.getMessage());
		}
		catch (IOException e) {
			logger.debug("WARCNAME: " + warcURL+ " " + e.getMessage());
		}
		catch(Exception e){
			logger.debug("Unhandled exception?" + e.getMessage());
		} finally{
			logger.debug("records: " + records);
			logger.debug("errors: " + errors);
			if(reader!=null){
				try {
					reader.close();
				} catch (IOException e) {
					logger.debug("error closing ArchiveReader"+ e.getMessage());
				}
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

		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4);
		job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum of 500 simultaneous maps accessing preprod for now*/

		// Sets reducer tasks to 1
		job.setNumReduceTasks(1);

		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
