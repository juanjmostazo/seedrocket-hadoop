package com.seedrocket.hadoop.example.wordsorter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordSorterPhase2Mapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

  /***
   * Input key (IntWritable): Total number of ocurrences
   * Input value (Text): Word
   * Intermediate key (IntWritable): Total number of ocurrences
   * Intermediate value (Text): Word
   */
  private IntWritable keyInt = new IntWritable();
  private Text valueText = new Text();

  @Override
  protected void map(LongWritable lineIndex, Text line, Context context)
          throws IOException, InterruptedException {
    /***
     * Split given read line into array of tokens
     */
    String[] tokens = line.toString().split("\\s+");

    /***
     * Get word and its number of ocurrences
     * Line format example: "123 Hello"
     */
    int numOcurrences = Integer.parseInt(tokens[0]);
    String word = tokens[1];

    /***
     * Set hadoop wrapper key/value
     */
    keyInt.set(numOcurrences);
    valueText.set(word);

    context.write(keyInt, valueText);
  }
}
