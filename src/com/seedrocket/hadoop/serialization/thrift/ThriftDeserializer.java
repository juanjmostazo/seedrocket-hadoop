/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.seedrocket.hadoop.serialization.thrift;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/***
 *
 * @author juanjo
 * DESERIALIZE METHOD HAS BEEN MODIFIED IN ORDER TO TO CLEAR GIVEN OBJECT
 */
public class ThriftDeserializer<T extends TBase> implements Deserializer<T> {

  private Class<T> tClass;
  private TIOStreamTransport transport;
  private TProtocol protocol;

  public ThriftDeserializer(Class<T> tBaseClass) {
    this.tClass = tBaseClass;
  }

  @Override
  public void open(InputStream in) {
    transport = new TIOStreamTransport(in);
    protocol = new TBinaryProtocol(transport);
  }

  @Override
  public T deserialize(T t) throws IOException {
    T object;

    if (t == null){
      object = newInstance();

    } else {
      t.clear();
      object = t;
    }

    try {
      object.read(protocol);

    } catch (TException e) {
      throw new IOException(e.toString());
    }

    return object;
  }

  @SuppressWarnings("unchecked")
  private T newInstance() {
    return (T) ReflectionUtils.newInstance(tClass, null);
  }

    @Override
  public void close() throws IOException {
    if (transport != null) {
      transport.close();
    }
  }
}

