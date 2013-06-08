package com.seedrocket.hadoop.example.wordcounter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordCounterReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

  /***
   * Intermediate key (Text): Word
   * Intermediate value (IntWritable): Number of word occurrences
   * Output key (Text):  Word
   * Output value (IntWritable): Total number of occurrences
   */
  private IntWritable valueInt = new IntWritable();

  @Override
  protected void reduce(Text word, Iterable<IntWritable> ocurrences, Context context)
          throws IOException, InterruptedException {
    /***
     * Iterates the array and count number of ocurrences
     */
    int numOcurrences = 0;
    for (IntWritable ocurrence : ocurrences) {
      numOcurrences += ocurrence.get();
    }

    /***
     * Emit stuff and its number of ocurrences
     */
    valueInt.set(numOcurrences);
    context.write(word, valueInt);
  }
}
