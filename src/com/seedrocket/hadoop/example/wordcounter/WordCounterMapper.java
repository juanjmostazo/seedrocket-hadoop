package com.seedrocket.hadoop.example.wordcounter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordCounterMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

  /***
   * Input key (LongWritable): Line seek pointer
   * Input value (Text): Line text
   * Intermediate key (Text): Word
   * Intermediate value (IntWritable): Number of word occurrences
   */
  private Text keyText = new Text();
  private IntWritable valueInt = new IntWritable();

  @Override
  protected void map(LongWritable lineIndex, Text line, Context context)
          throws IOException, InterruptedException {
    /***
     * Split given read line into array of tokens
     */
    String[] tokens = line.toString().split("\\s+");

    /***
     * Iterates array of tokens and emit each {token, 1}
     */
    for (String token : tokens) {
      keyText.set(token);
      valueInt.set(1);

      context.write(keyText, valueInt);
    }
  }
}
