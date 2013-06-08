package com.seedrocket.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
 *
 * @author juanj.mostazo@gmail.com
 */
public class HadoopDriver extends ProgramDriver {

  public static void main(String... args) throws Throwable {
    ProgramDriver driver = new ProgramDriver();
    
    driver.addClass(
            "wordCounter",
            com.seedrocket.hadoop.example.wordcounter.WordCounter.class,
            "Launches the word counter test app");

    driver.addClass(
            "wordSorter",
            com.seedrocket.hadoop.example.wordsorter.WordSorter.class,
            "Launches the word sorter test app");

    driver.addClass(
            "dupDetector",
            com.seedrocket.hadoop.example.dupdetector.DupDetector.class,
            "Launches the contact dup detector test app");

    driver.addClass(
            "contactFileGenerator",
            com.seedrocket.hadoop.util.ContactFileGenerator.class,
            "Launches the contact file generator");

    driver.driver(args);
  }
}
