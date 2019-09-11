import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;


public class ImageDBOutputWritable implements Writable, DBWritable
{
	
   private String image_hash_key;
   private String url;
   private long tstamp;
   private String mime;
   private String collection;
   private int safe;
   private String content_hash;

   public ImageDBOutputWritable(String image_hash_key, String url, long tstamp, String mime,String collection, int safe, String content_hash)
   {
	   this.image_hash_key = image_hash_key;
	   this.url = url;
	   this.tstamp = tstamp;
	   this.mime = mime;
	   this.collection = collection;
	   this.safe = safe;
	   this.content_hash = content_hash;
   }

   public void readFields(DataInput in) throws IOException {   }

   public void readFields(ResultSet rs) throws SQLException
   {
	 image_hash_key = rs.getString(1);
	 url = rs.getString(2);
	 tstamp = rs.getLong(3);
	 mime = rs.getString(4);
	 collection = rs.getString(5);
	 safe = rs.getInt(6);
     content_hash =  rs.getString(7);
   }

   public void write(DataOutput out) throws IOException {    }

   public void write(PreparedStatement ps)
   {
     try {
    	 ps.setString(1, image_hash_key);
         ps.setString(2, url);
         ps.setLong(3, tstamp);
         ps.setString(4, mime);
         ps.setString(5, collection);
         ps.setInt(6, safe);
         ps.setString(7, content_hash);
	} catch (SQLException e) {
	    if (e instanceof SQLIntegrityConstraintViolationException) {
	    	System.out.println("SQL duplicate key: " + e );
	    } else {		
	    	System.out.println("SQL error: " + e );
	    	e.printStackTrace();
	    }
	}

           
   }
}