package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    double sum = 0;
    int count = 0;
    
    // Iterate through all the lengths associated with letter
    for (IntWritable value : values) {
        sum += value.get();
        count++;
    }
    
    // Calculate the average and emit the final key, value pair
    if (count > 0) {
        double average = sum / count;
        context.write(key, new DoubleWritable(average));
    }
  }
}

// Direct implementation of the Reducer from Assignment 2.docx