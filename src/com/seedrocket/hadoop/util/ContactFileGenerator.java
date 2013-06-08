package com.seedrocket.hadoop.util;

import com.seedrocket.hadoop.serialization.thrift.Contact;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author juanjo
 */
public class ContactFileGenerator {

  public static String NAMES[] = {"Hamilcar", "Hannibal", "Hasdrubal", "Magon"};
  public static int NUM_PHONE_DIGITS = 9;
  public static int LIST_ELEMS = 10000;

  public static void main (String[] args) throws IOException{
    System.out.println("Launching Contact File Generator");

    /***
     * Define input file and output folder
     */
    Path outputFile = new Path("seedrocket/input/contactlist/list.seq");

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
     * Delete output file (if exists)
     */
    fs.delete(outputFile, true);

    /***
     * Initializes writer, thrift and auxiliar objects
     */
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, outputFile, NullWritable.class, Contact.class);
    Contact contact = new Contact();
    Random random = new Random();

    /***
     * Initializes generator random algorithm
     */
    for (int i=0; i<LIST_ELEMS; i++){
      contact.clear();
      contact.setId(i);
      contact.setName(getRandomName(random));
      contact.setPhone(getRandomPhone(random));

      System.out.println("Appending contact to sequence file: " + contact);

      writer.append(NullWritable.get(), contact);
    }

    /***
     * Closes writer
     */
    writer.close();

    System.out.println("Contact File generated successfully at '" + outputFile + "'");
  }

  public static String getRandomName(Random random){
    return NAMES[Math.abs(random.nextInt()) % NAMES.length];
  }

  public static long getRandomPhone(Random random){
    StringBuilder phone = new StringBuilder();

    for (int i=0; i<NUM_PHONE_DIGITS; i++){
      phone.append(Math.abs(random.nextInt()) % 10);
    }

    return Long.parseLong(phone.toString());
  }
}
