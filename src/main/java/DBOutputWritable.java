import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable
{
   private String name;
   private int count;

   public DBOutputWritable(String name, int count)
   {
     this.name = name;
     this.count = count;
   }

   public void readFields(DataInput in) throws IOException {   }

   public void readFields(ResultSet rs) throws SQLException
   {
     name = rs.getString(1);
     count = rs.getInt(2);
   }

   public void write(DataOutput out) throws IOException {    }

   public void write(PreparedStatement ps) throws SQLException
   {
     ps.setString(1, name);
     ps.setInt(2, count);
   }
}