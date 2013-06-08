package com.seedrocket.hadoop.example.wordcounter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordCounter {

  public static void main (String [] args) throws IOException, InterruptedException, ClassNotFoundException{
    System.out.println("Launching WordCounter test app");
    
    /***
     * Define input file and output folder
     */
    Path inputFolder = new Path("seedrocket/input/books");
    Path outputFolder = new Path("seedrocket/examples/wordcounter/output");
    
    /***
     * Creates new configuration and initializes file system
     */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    /***
     * Check if input path exists
     */
    if (!fs.exists(inputFolder)){
      System.err.println("Input folder '" + inputFolder + "' does not exist on FS");
      System.exit(-1);
    }

    /***
     * Delete output folder (if exists)
     * Otherwise M/R job will crash
     */
    fs.delete(outputFolder, true);

    /***
     * Initializes hadoop job
     */
    Job job = new Job(conf);
    job.setJobName("WordCounter");
    job.setJarByClass(WordCounter.class);

    /***
     * Set Mapper and Reducer class
     */
    job.setMapperClass(WordCounterMapper.class);
    job.setReducerClass(WordCounterReducer.class);

    /***
     * Set job input and output file formats
     */
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    /***
     * Set intermediate key/value types
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    /***
     * Set output key/value types
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    /***
     * Specifies input and output paths
     */
    FileInputFormat.addInputPath(job, inputFolder);
    FileOutputFormat.setOutputPath(job, outputFolder);

    /***
     * Set number of reduce tasks
     */
    job.setNumReduceTasks(1);

    /***
     * Runs job
     */
    job.submit();

    /***
     * Waits until jobs has finished completely or failed
     */
    if (job.waitForCompletion(true)){
      System.out.println("Job executed successfully!");
    } else {
      System.out.println("Fail executing job");
    }
  }
}
