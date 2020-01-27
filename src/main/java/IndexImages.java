import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kerby.util.Base64;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;
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
import com.mongodb.ServerAddress;

public class IndexImages
{
	public static class Map extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		private static Logger logger = Logger.getLogger(Map.class);
		public static String collection;

		private MongoClient mongoClient;
		private DBCollection imagesMongoDB;
		private DBCollection imageIndexesMongoDB;
		private HashSet<String> nullImageHashes ;

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			collection = config.get("collection");
			System.out.println("collection: " + collection);

			String mongodbServers = config.get("mondodb.servers");
			List<ServerAddress> mongodbServerSeeds = ImageSearchIndexingUtil.getMongoDBServerAddresses(mongodbServers);

			MongoClientOptions.Builder options = MongoClientOptions.builder();
			options.socketKeepAlive(true);
			mongoClient = new MongoClient(mongodbServerSeeds, options.build());
			DB database = mongoClient.getDB("hadoop_images");
			imagesMongoDB = database.getCollection("images");
			imageIndexesMongoDB = database.getCollection("imageIndexes");
			nullImageHashes = new HashSet<String>();

			logger.debug(collection+"_Images"+"/img/");
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			mongoClient.close();
			super.cleanup(context);
		}

		public  void parseImagesFromHtmlRecord(Context context, byte[] arcRecordBytes, String pageURL, String pageTstamp){
			try{
				logger.debug("Parsing Images from (W)ARCrecord");
				logger.debug("Number of content bytes: " + arcRecordBytes.length);
				logger.debug("URL: "+ pageURL);
				logger.debug("Page TS: "+pageTstamp);

				logger.info("Parsing Images from (W)ARCrecord" );
				logger.info("Read Content Bytes from (W)ARCrecord" );
				String recordEncoding = ImageSearchIndexingUtil.guessEncoding(arcRecordBytes);
				InputStream is = new ByteArrayInputStream(arcRecordBytes);

				Document doc = Jsoup.parse(is, recordEncoding, "");
				String pageTitle = doc.title(); /*returns empty string if no title in html document*/

				Elements imgs = doc.getElementsByTag("img");
				int pageImages = imgs.size();

				logger.debug("Page contains: "+ pageImages + " images");

				String pageURLCleaned = URLDecoder.decode(pageURL, "UTF-8"); /*Escape URL e.g %C3*/
				pageURLCleaned = StringUtils.stripAccents(pageURLCleaned); /* Remove accents*/
				String pageURLTokens = ImageSearchIndexingUtil.parseURL(pageURLCleaned); /*split the URL*/


				URL uri = new URL(pageURL);
				String pageHost = uri.getHost();
				String pageProtocol = uri.getProtocol();
				//String pageTstamp = record.getMetaData().getDate();
				if (pageTstamp == null || pageTstamp.equals("")){
					logger.debug("Null pageTstamp");
				}
				logger.debug("pageTstamp:" + pageTstamp);


				for(Element el: imgs){
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
						logger.debug("URL of image too big ");
						logger.debug(pageURL.substring(0,500) + "...");
						continue;
					}/*Maximum size for SOLR index is 10 000*/
					if (imgSrc == null || imgSrc.equals("")){
						logger.debug("Null imgSrc");
						continue;
					}
					String imgDigest = DigestUtils.md5Hex(imgSrc);

					if(nullImageHashes.contains(imgDigest)){
						/*Image was not retrieved in this collection skip*/
						continue;
					}else{
						ImageSQLDTO imgSQLDTO = retreiveImageFromDB(imgDigest, Long.parseLong(pageTstamp), context.getConfiguration());
						if(imgSQLDTO == null){
							logger.debug("Got null image for hash: "+ imgDigest);
							nullImageHashes.add(imgDigest);
							continue;
						}
						ImageSearchResult imgResult = ImageParse.getPropImage(imgSQLDTO);
						if ( imgResult == null ){
							logger.debug("Failed to create thumbnail for image: "+ imgSrc + "with ts: "+imgSQLDTO.getTstamp());
							continue;
						}

						String imgSrcCleaned = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
						imgSrcCleaned = StringUtils.stripAccents(imgSrcCleaned); /* Remove accents*/
						String imgSrcTokens = ImageSearchIndexingUtil.parseURL(imgSrcCleaned); /*split the imgSrc URL*/

						String imgTitle = el.attr("title");
						if(imgTitle.length() > 9999){imgTitle =  imgTitle.substring(0, 10000); }
						String imgAlt = el.attr("alt");
						if(imgAlt.length() > 9999){imgAlt =  imgAlt.substring(0, 10000); }

						insertImageIndexes(imgResult, imgSrc,imgSQLDTO, imgSrcTokens,imgTitle, imgAlt, pageImages, pageTstamp, pageURL, pageHost, pageProtocol,  pageTitle, pageURLTokens);

						logger.debug("Written to file - successfully indexed image record" );

					}
				}
			} catch (Exception e){
				logger.debug("Something failed JSOUP parsing " + e.getMessage() );
			}

		}


		/* Retreives ImageSQLDTO with the closest date to pageTstamp
		 * Returns null if no results found in the SQL DB
		 *
		 */
		public ImageSQLDTO retreiveImageFromDB(String imgHashKey, long pageTstamp, Configuration conf) {
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("_id.image_hash_key", imgHashKey);
			DBCursor cursor = imagesMongoDB.find(whereQuery);

			long tstampDiff = 99999999999999L; /*Difference between pageStamp and imgTstamp, we want the closest possible*/
			long imgTstamp = -1;
			long currentTstamp = -1;
			String mime ="";
			byte[] retrievedImgBytes = null;

			while (cursor.hasNext()) {
				DBObject currentResult = cursor.next();
				String ctstamp =(String) currentResult.get("tstamp");
				if(ctstamp == null) {
					logger.debug("Empty tstamp for image with hash: " + imgHashKey);
					return null;
				};
				currentTstamp = Long.parseLong(ctstamp);
				if(Math.abs(currentTstamp - pageTstamp) < tstampDiff){
					imgTstamp = currentTstamp;
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
			DBCursor cursor = imageIndexesMongoDB.find(whereQuery);
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
						.append( "collection" , collection );

				if(numberofImagesWithHashKey == 0){ /*insert image it is unique in our imageIndexes collection*/
					imageIndexesMongoDB.insert(img);
					logger.debug("inserted:" + imgResult.getDigest());
					logger.debug("inserted ts:" + imgSQLDTO.getTstamp());
				}
				else if (numberofImagesWithHashKey == 1){ /*check if this image tstamp is more old than the one we currently store if it is update this record in the db*/
					DBObject currentResult = cursor.next(); /*get the record*/
					String dataBaseTstamp =(String) currentResult.get("imgTstamp");
					if(dataBaseTstamp == null) {
						logger.debug("Empty tstamp for image with hash: " + imgDigest);
						return;
					}
					Long dBTstamp = Long.parseLong(dataBaseTstamp);
					if(imgTstampLong <= dBTstamp){ /*Found an older version of this image digest lets update the database*/
						imageIndexesMongoDB.update(whereQuery, img);
						logger.debug("updated: " + imgResult.getDigest());
						logger.debug("updated TS: " + imgSQLDTO.getTstamp());
					}
					else{ /*There is an older version of this digest in our db so we won't insert this one*/
						return;
					}
				}
			}
			else{
				/*This should never happen because imgDigest is unique in our DB*/
				logger.debug("Multiple records with hash key: " + imgDigest);
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String warcURL = value.toString();
			System.out.println("(W)ARCNAME: " + warcURL);
			logger.info("Map Started for (W)ARCNAME: " + warcURL);

			try{
				logger.debug("(W)ARCNAME: " + warcURL);

				if(warcURL.endsWith("warc.gz") || warcURL.endsWith("warc")){
					logger.debug("READING WARC");
					ImageSearchIndexingUtil.readWarcRecords(warcURL, record -> {
						if(record!= null && record.getContentMimetype() != null && record.getContentMimetype().contains("html")){ /*only processing images*/
							logger.debug("Searching images in html record");
							parseImagesFromHtmlRecord(context, record.getContentBytes(), record.getWARCRecord().getHeader().getUrl(), record.getTs());
						}
					});
				}else{
					logger.debug("READING ARC");

					ImageSearchIndexingUtil.readArcRecords(warcURL, record -> {
						if(record.getMetaData().getMimetype().contains("html")) {
							byte[] recordContentBytes;
							try {
								recordContentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
							} catch (IOException e) {
								logger.error(String.format("Error getting record content bytes for (w)arc: %s on offset %d with error message %s", warcURL, record.getBodyOffset(), e.getMessage()));
								return;
							}
							parseImagesFromHtmlRecord(context, recordContentBytes, record.getHeader().getUrl(), record.getMetaData().getDate());
						}
					});
				}

				context.write(new LongWritable(0L), NullWritable.get()); //dumb code write 0 , NULLWritable to send to the reducer phase

			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				logger.debug("ARCNAME: " + warcURL+ " "+e.getMessage());
			}
			catch(Exception e){
				logger.debug("Unhandled exception? " + e.getMessage());
			}
		}
	}

	public static class IndexReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		private static Logger logger = Logger.getLogger(IndexReducer.class);

		public String collectionName;
		private MongoClient mongoClient;
		private DBCollection images;

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			collectionName = config.get("collection");
			logger.debug("collection: " + collectionName);
			MongoClientOptions.Builder options = MongoClientOptions.builder();
			options.socketKeepAlive(true);
			mongoClient = new MongoClient( Arrays.asList(
					new ServerAddress("p37.arquivo.pt", 27020),
					new ServerAddress("p38.arquivo.pt", 27020),
					new ServerAddress("p39.arquivo.pt", 27020)), options.build());
			DB database = mongoClient.getDB("hadoop_images");
			images = database.getCollection("images");
		}

		public void reduce(LongWritable key, Iterable<NullWritable> values,
				Context context
				) throws IOException, InterruptedException {
			/*We are removing all records from images db within the collection we have just indexed to clean space*/
			logger.debug("Reduce IndexImages");
			BasicDBObject query = new BasicDBObject();
			query.append("collection", collectionName);
			images.remove(query);

			context.write(new LongWritable(0L), NullWritable.get()); //dumb code write 0 , NULLWritable to finish reduce phase
		}

		@Override
		protected void cleanup(Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			mongoClient.close();
			super.cleanup(context);
		}
	}

	public static void main( String[] args ) throws Exception
	{
		assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
		String hdfsArcsPath = args[0];

		assert args.length >= 2 : "Missing collection name argument";
		String collection = args[1];
		String jobName = collection + "_IndexImages_2";

		assert args.length >= 3 : "Missing mondo DB servers connection string argument";
		String mongodbServers = args[2];

		assert args.length >= 4 : "Missing argument max running map in parallel";
		int maxMaps = Integer.parseInt(args[3]);

		assert args.length >= 5 : "Missing argument max arcs per map";
		int linespermap = Integer.parseInt(args[4]);

		Configuration conf = new Configuration();
		conf.set("collection", collection);
		conf.set("mondodb.servers", mongodbServers);

		Job job = Job.getInstance(conf, "Index Images");
		job.setJarByClass(IndexImages.class);
		job.setMapperClass(Map.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(IndexReducer.class);
		job.setJobName(jobName);
		job.setInputFormatClass(NLineInputFormat.class);

		NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));

		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
		job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

		// Sets reducer tasks to 1
		job.setNumReduceTasks(1);

		FileOutputFormat.setOutputPath(job, new Path(hdfsArcsPath));

		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
