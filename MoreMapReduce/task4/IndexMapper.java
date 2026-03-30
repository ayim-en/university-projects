package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    
    // Get the name of the file (the play title)
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    String fileName = fileSplit.getPath().getName();
    
    // The key is the line number, the value is the line of text
    String line = value.toString();
    
    // Split the line into words
    for (String word : line.split("\\W+")) {
      if (word.length() > 0) {
        // Emit word as key, and "playname:linenumber" as value
        context.write(new Text(word.toLowerCase()), new Text(fileName + ":" + key.toString()));
      }
    }
  }
}