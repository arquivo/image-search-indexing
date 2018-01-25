import hadoopImageParser.ImageSearchResult;
import hadoopImageParser.ImageParse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLDecoder;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
 
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.ArchiveRecord;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.commons.lang3.StringUtils;




class Map extends Mapper<LongWritable, Text, Text, Text> {

    public static String collectionName;

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        collectionName = config.get("collection");
        System.out.println("collection: " + collectionName);
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


    public  void parseImagesFromHtmlRecord(ARCRecord record, Context context) throws IOException{
        OutputStream output = new ByteArrayOutputStream()
        {
            private StringBuilder string = new StringBuilder();
            //Netbeans IDE automatically overrides this toString()
            public String toString(){
                return this.string.toString();
            }
        };                    
        record.dump(output);

        output.close();
        byte[] arcRecordBytes = ((ByteArrayOutputStream) output).toByteArray();

        String recordEncoding = guessEncoding(arcRecordBytes); 

        try{

			InputStream is = new ByteArrayInputStream(arcRecordBytes);  

            Document doc = Jsoup.parse(is, recordEncoding, "");
            String pageTitle = doc.title(); /*returns empty string if no title in html document*/

            Elements imgs = doc.getElementsByTag("img");
            int pageImages = imgs.size();
            String pageURL = record.getHeader().getUrl();
            String pageURLDigest = ImageParse.hash256(pageURL);
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

                ImageSearchResult imgResult = ImageParse.getPropImage("http://preprod.arquivo.pt/wayback/"+pageTstamp+"/"+imgSrc);
                if ( imgResult == null ){
                    System.err.println("Not Found Image URL: http://preprod.arquivo.pt/wayback/"+pageTstamp+"/"+imgSrc );
                    continue;
                }
                String imgSrcCleaned = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
                imgSrcCleaned = StringUtils.stripAccents(imgSrcCleaned); /* Remove accents*/
                String imgSrcTokens = parseURL(imgSrcCleaned); /*split the imgSrc URL*/                

                obj.put( "imgWidth", imgResult.getWidth( ) ); 
                obj.put( "imgHeight", imgResult.getHeight( ) ); 
                obj.put( "imgSrc", imgSrc); /*The URL of the Image*/
                obj.put( "imgSrcTokens", imgSrcTokens);
                obj.put( "imgSrcURLDigest", ImageParse.hash256(imgSrc)); /*Digest Sha-256 of the URL of the Image*/
                
                if(el.attr("title").length() > 9999){
                    obj.put( "imgTitle", el.attr("title").substring(0,10000) );
                }
                else{
                    obj.put( "imgTitle", el.attr("title") );
                }
                if(el.attr("alt").length() > 9999){
                    obj.put( "imgAlt", el.attr("alt").substring(0,10000) );
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
            }
        }catch (Exception e){
            System.err.println("Something failed JSOUP parsing");
            e.printStackTrace();
        }
      
    }



	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
        ARCReader reader = null;
        try {
            int records = 0;
            int errors = 0;
            
            System.out.println("ARCNAME: " + value.toString());
            reader = ARCReaderFactory.get(value.toString());


            for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
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
        }				
	}

}


public class IndexImages 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
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

	job.setOutputFormatClass(TextOutputFormat.class);
		
	// Sets reducer tasks to 0
	job.setNumReduceTasks(0);

	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean result = job.waitForCompletion(true);

	System.exit(result ? 0 : 1);
    }
}
