package com.seedrocket.hadoop.example.wordsorter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordSorterPhase1Reducer
        extends Reducer<Text, IntWritable, IntWritable, Text> {

  /***
   * Intermediate key (Text): Word
   * Intermediate value (IntWritable): Number of word occurrences
   * Output key (IntWritable): Total number of occurrences
   * Output value (Text): Word
   */
  private IntWritable keyInt = new IntWritable();

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
    keyInt.set(numOcurrences);
    context.write(keyInt, word);

    /***
     * Just for stats, count number of total words
     */
    context.getCounter("STATS", "TOTAL_WORDS").increment(1L);
  }
}
