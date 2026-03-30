package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	  String line = value.toString();
	  String[] parts = line.split(" ");
	  
	  if (parts.length > 3) {
		  String ip = parts[0];
		  String[] dateParts = parts[3].split("/");
		  if (dateParts.length > 1) {
			  String month = dateParts[1];
			  context.write(new Text(ip), new Text(month));
		  }
	  }
  }
}