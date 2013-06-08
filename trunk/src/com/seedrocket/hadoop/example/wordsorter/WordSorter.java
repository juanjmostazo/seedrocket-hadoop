package com.seedrocket.hadoop.example.wordsorter;

import com.seedrocket.hadoop.util.IntWritableReverseSortComparator;
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
public class WordSorter {

  private static Configuration conf;
  private static FileSystem fs;
  private static Path inputFolder;
  private static Path outputFolderPhase1;
  private static Path outputFolder;
  private static Job job1;
  private static Job job2;

  public static void main (String [] args) throws IOException, InterruptedException, ClassNotFoundException{
    System.out.println("Launching WordSorter test app");
    
    setUpFileSystemStuff();

    setUpJob1();

    setUpJob2();

    runPipelineOfJobs();
  }

  private static void setUpFileSystemStuff() throws IOException{
    /***
     * Define input file and output folder
     */
    inputFolder = new Path("seedrocket/input/books");
    outputFolderPhase1 = new Path("seedrocket/examples/wordsorter/outputphase1");
    outputFolder = new Path("seedrocket/examples/wordsorter/output");

    /***
     * Creates new configuration and initializes file system
     */
    conf = new Configuration();
    fs = FileSystem.get(conf);

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
    fs.delete(outputFolderPhase1, true);

    /***
     * Delete output folder (if exists)
     * Otherwise M/R job will crash
     */
    fs.delete(outputFolder, true);
  }

  private static void setUpJob1() throws IOException{
    /***
     * Initializes hadoop job
     */
    job1 = new Job(conf);
    job1.setJobName("WordSorterPhase1");
    job1.setJarByClass(WordSorter.class);

    /***
     * Set Mapper and Reducer class
     */
    job1.setMapperClass(WordSorterPhase1Mapper.class);
    job1.setReducerClass(WordSorterPhase1Reducer.class);

    /***
     * Set job input and output file formats
     */
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    /***
     * Set intermediate key/value types
     */
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);

    /***
     * Set output key/value types
     */
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Text.class);

    /***
     * Specifies input and output paths
     */
    FileInputFormat.addInputPath(job1, inputFolder);
    FileOutputFormat.setOutputPath(job1, outputFolderPhase1);

    /***
     * Set number of reduce tasks
     */
    job1.setNumReduceTasks(1);
  }

  private static void setUpJob2() throws IOException{
    /***
     * Initializes hadoop job
     */
    job2 = new Job(conf);
    job2.setJobName("WordSorterPhase2");
    job2.setJarByClass(WordSorter.class);

    /***
     * Set Mapper and Reducer class
     */
    job2.setMapperClass(WordSorterPhase2Mapper.class);
    job2.setReducerClass(WordSorterPhase2Reducer.class);

    /***
     * Set job input and output file formats
     */
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    /***
     * Set intermediate key/value types
     */
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);

    /***
     * Set output key/value types
     */
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    /***
     * Specifies input and output paths
     */
    FileInputFormat.addInputPath(job2, outputFolderPhase1);
    FileOutputFormat.setOutputPath(job2, outputFolder);

    /***
     * Set number of reduce tasks
     */
    job2.setNumReduceTasks(1);

    /***
     * Optional: Reverse Sort Comparator
     */
    job2.setSortComparatorClass(IntWritableReverseSortComparator.class);
  }

  private static void runPipelineOfJobs() throws IOException, InterruptedException, ClassNotFoundException{
    /***
     * Runs job1
     */
    job1.submit();

    /***
     * Waits until job has finished completely or failed
     */
    if (!job1.waitForCompletion(true)){
      System.err.println("Fail executing job1");
      System.exit(-1);
    }

    /***
     * Runs job2
     */
    job2.submit();

    /***
     * Waits until job has finished completely or failed
     */
    if (!job2.waitForCompletion(true)){
      System.err.println("Fail executing job2");
      System.exit(-1);
    }

    System.out.println("Pipeline of jobs executed successfully");
  }
}
