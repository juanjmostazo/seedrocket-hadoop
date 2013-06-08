package com.seedrocket.hadoop.example.dupdetector;

import com.seedrocket.hadoop.serialization.thrift.Contact;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class DupDetectorReducer
        extends Reducer<Text, Contact, NullWritable, Contact> {

  /***
   * Intermediate key (Text): Hash from contact fields
   * Intermediate value (Contact): Contact thrift object
   * Output key (Text): Hash from contact fields
   * Output value (NullWritable): No key
   */
  @Override
  protected void reduce(Text hash, Iterable<Contact> contacts, Context context)
          throws IOException, InterruptedException {
    /***
     * Iterates grouped contacts by hash
     */
    boolean first = true;
    for (Contact contact : contacts) {
      /***
       * Only emits contact if it is the first one, otherwise contact is skipped
       */
      if (first) {
        first = false;
        context.write(NullWritable.get(), contact);

        /***
         * Just for stats, count number of skipped contacts
         */
        context.getCounter("STATS", "TOTAL_EMITTED").increment(1L);

        /***
         * This shouldn't be done
         */
        System.out.println("Emitted: " + contact);
      } else {
        /***
         * Just for stats, count number of skipped contacts
         */
        context.getCounter("STATS", "TOTAL_SKIPPED").increment(1L);
      }
    }
  }
}
