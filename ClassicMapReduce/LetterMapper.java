package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // Convert the input Text to a String
    String line = value.toString();
    // Split the line by one or more non-word characters
    String[] words = line.split("\\W+");
    
    for (String word : words) {
        if (word.length() > 0) {
            // Get the first letter of the word (case-sensitive)
            String firstLetter = word.substring(0, 1);
            // Get the length of the word
            int length = word.length();
            // Emit the first letter as the key and the word length as the value
            context.write(new Text(firstLetter), new IntWritable(length));
        }
    }
  }
}

// Direct implemntation of the Mapper from Assignment 2.docx 