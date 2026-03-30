package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;


public class StringPairTestDriver {

    public static void main(String[] args) throws Exception {

       if (args.length != 2) {
	      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Custom Writable Comparable");
        job.setJarByClass(StringPairTestDriver.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * LongSumReducer is a Hadoop API class that sums values into
         * A LongWritable.  It works with any key and value type, therefore
         * supports the new StringPairWritable as a key type.
         */
        job.setReducerClass(LongSumReducer.class);

        job.setMapperClass(StringPairMapper.class);
    
	/*
	 * Set the key output class for the job
	 */   
        job.setMapOutputKeyClass(StringPairWritable.class);
        job.setOutputKeyClass(StringPairWritable.class);
    
        /*
         * Set the value output class for the job
         */
        job.setOutputValueClass(LongWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
  }
}