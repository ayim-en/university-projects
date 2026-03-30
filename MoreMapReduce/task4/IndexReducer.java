package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    
    for (Text value : values) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(value.toString());
      first = false;
    }
    
    context.write(key, new Text(sb.toString()));
  }
}