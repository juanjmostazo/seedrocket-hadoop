package com.seedrocket.hadoop.example.dupdetector;

import com.seedrocket.hadoop.serialization.thrift.Contact;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class DupDetectorMapper
        extends Mapper<NullWritable, Contact, Text, Contact> {

  /***
   * Input key (NullWritable): No key
   * Input value (Contact): Contact thrift object
   * Intermediate key (Text): Hash from contact fields
   * Intermediate value (Contact): Contact thrift object
   */
  private Text keyText = new Text();

  @Override
  protected void map(NullWritable nullKey, Contact contact, Context context)
          throws IOException, InterruptedException {
    /***
     * Sets intermediate making hash from contact fields
     */
    setHashFunction(contact);

    /***
     * Emits stuff to shuffle and reducer
     */
    context.write(keyText, contact);
  }

  private void setHashFunction(Contact contact) {
    keyText.set(contact.getName());
  }
}
