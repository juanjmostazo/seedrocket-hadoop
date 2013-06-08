package com.seedrocket.hadoop.example.dupdetector;

import com.seedrocket.hadoop.serialization.thrift.Contact;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class DupDetector {

  public static void main (String [] args) throws IOException, InterruptedException, ClassNotFoundException{
    System.out.println("Launching DupDetector test app");
    
    /***
     * Define input file and output folder
     */
    Path inputFolder = new Path("seedrocket/input/contactlist/list.seq");
    Path outputFolder = new Path("seedrocket/examples/dupdetector/output");
    
    /***
     * Creates new configuration and initializes file system
     */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    /***
     * Registers thrift serialization
     */
    conf.set("io.serializations",
            conf.get("io.serializations") + "," + "com.seedrocket.hadoop.serialization.thrift.ThriftSerialization");
    
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
    job.setJobName("DupDetector");
    job.setJarByClass(DupDetector.class);

    /***
     * Set Mapper and Reducer class
     */
    job.setMapperClass(DupDetectorMapper.class);
    job.setReducerClass(DupDetectorReducer.class);

    /***
     * Set job input and output file formats
     */
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    /***
     * Set intermediate key/value types
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Contact.class);

    /***
     * Set output key/value types
     */
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Contact.class);

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
