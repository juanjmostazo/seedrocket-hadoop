package com.seedrocket.hadoop.util;

import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author juanjo
 */
public class IntWritableReverseSortComparator extends IntWritable.Comparator{

  @Override
  public int compare(Object a, Object b) {
    return 0 - super.compare(a, b);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return 0 - super.compare(b1, s1, l1, b2, s2, l2);
  }
}
