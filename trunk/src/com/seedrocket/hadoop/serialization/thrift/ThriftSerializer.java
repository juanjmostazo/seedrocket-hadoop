/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.seedrocket.hadoop.serialization.thrift;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class ThriftSerializer implements Serializer<TBase> {

  private TIOStreamTransport transport;
  private TProtocol protocol;

  public void open(OutputStream out) {
    transport = new TIOStreamTransport(out);
    protocol = new TBinaryProtocol(transport);
  }

  public void serialize(TBase tBase) throws IOException {
    try {
      tBase.write(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public void close() throws IOException {
    if (transport != null) {
      transport.close();
    }
  }

}