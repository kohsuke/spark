/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.*;
import java.io.*;
import java.net.URI;
import java.sql.*;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Utilities.
 *
 */
@SuppressWarnings("nls")
public final class Utilities {

  /**
   * Kryo serializer for timestamp.
   */
  private static class TimestampSerializer extends
          com.esotericsoftware.kryo.Serializer<Timestamp> {

    @Override
    public Timestamp read(Kryo kryo, Input input, Class<Timestamp> clazz) {
      Timestamp ts = new Timestamp(input.readLong());
      ts.setNanos(input.readInt());
      return ts;
    }

    @Override
    public void write(Kryo kryo, Output output, Timestamp ts) {
      output.writeLong(ts.getTime());
      output.writeInt(ts.getNanos());
    }
  }

  /** Custom Kryo serializer for sql date, otherwise Kryo gets confused between
   java.sql.Date and java.util.Date while deserializing
   */
  private static class SqlDateSerializer extends
          com.esotericsoftware.kryo.Serializer<java.sql.Date> {

    @Override
    public java.sql.Date read(Kryo kryo, Input input, Class<java.sql.Date> clazz) {
      return new java.sql.Date(input.readLong());
    }

    @Override
    public void write(Kryo kryo, Output output, java.sql.Date sqlDate) {
      output.writeLong(sqlDate.getTime());
    }
  }

  /**
   * Serializes expression via Kryo.
   * @param expr Expression.
   * @return Bytes.
   */
  public static byte[] serializeExpressionToKryo(ExprNodeGenericFuncDesc expr) {
    return serializeObjectToKryo(expr);
  }

  private static byte[] serializeObjectToKryo(Serializable object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    runtimeSerializationKryo.get().writeObject(output, object);
    output.close();
    return baos.toByteArray();
  }

  private static class PathSerializer extends com.esotericsoftware.kryo.Serializer<Path> {

    @Override
    public void write(Kryo kryo, Output output, Path path) {
      output.writeString(path.toUri().toString());
    }

    @Override
    public Path read(Kryo kryo, Input input, Class<Path> type) {
      return new Path(URI.create(input.readString()));
    }
  }

  // Kryo is not thread-safe,
  // Also new Kryo() is expensive, so we want to do it just once.
  public static ThreadLocal<Kryo> runtimeSerializationKryo = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      Kryo kryo = new Kryo();
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      kryo.register(java.sql.Date.class, new SqlDateSerializer());
      kryo.register(java.sql.Timestamp.class, new TimestampSerializer());
      kryo.register(Path.class, new PathSerializer());
      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
      removeField(kryo, Operator.class, "colExprMap");
      removeField(kryo, ColumnInfo.class, "objectInspector");
      removeField(kryo, AbstractOperatorDesc.class, "statistics");
      return kryo;
    }
  };

  @SuppressWarnings("rawtypes")
  protected static void removeField(Kryo kryo, Class type, String fieldName) {
    FieldSerializer fld = new FieldSerializer(kryo, type);
    fld.removeField(fieldName);
    kryo.register(type, fld);
  }

}
