package stubs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class AvgWordLength {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: AvgWordLength <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "avgwordlength");

    job.setJarByClass(AvgWordLength.class);
    
    // Set the input and output paths
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // Configure the job with the Mapper and Reducer classes
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
    
    // Set the data types for the intermediate output (Mapper output)
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Set the data types for the final output (Reducer output)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

// Direct implementation of the Driver from Assignment 2.docx