package com.seedrocket.hadoop.example.wordsorter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class WordSorterPhase2Reducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {

  /***
   * Intermediate key (IntWritable): Total number of ocurrences
   * Intermediate value (Text): Word
   * Output key (IntWritable): Total number of ocurrences
   * Output value (Text): Word
   */
  @Override
  protected void reduce(IntWritable numOcurrences, Iterable<Text> words, Context context)
          throws IOException, InterruptedException {

    /***
     * Identity reducer, just emits key and its values
     */

    int count = 100;

    for (Text word : words) {
      if (count > 0){
        context.write(numOcurrences, word);
      }

      count--;
      /***
       * Just for stats, count number of total words
       */
      context.getCounter("STATS", "TOTAL_WORDS").increment(1L);
    }
  }
}
