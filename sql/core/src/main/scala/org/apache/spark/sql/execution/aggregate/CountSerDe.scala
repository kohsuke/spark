/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.countSerDe

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.aggregate.UserDefinedImperativeAggregator
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[CountSerDeUDT])
case class CountSerDeSQL(nSer: Int, nDeSer: Int, sum: Double)

class CountSerDeUDT extends UserDefinedType[CountSerDeSQL] {
  def userClass: Class[CountSerDeSQL] = classOf[CountSerDeSQL]

  override def typeName: String = "count-ser-de"

  private[spark] override def asNullable: CountSerDeUDT = this

  def sqlType: DataType = StructType(
    StructField("nSer", IntegerType, false) ::
    StructField("nDeSer", IntegerType, false) ::
    StructField("sum", DoubleType, false) ::
    Nil)

  def serialize(sql: CountSerDeSQL): Any = {
    val row = new GenericInternalRow(3)
    row.setInt(0, 1 + sql.nSer)
    row.setInt(1, sql.nDeSer)
    row.setDouble(2, sql.sum)
    row
  }

  def deserialize(any: Any): CountSerDeSQL = any match {
    case row: InternalRow if (row.numFields == 3) =>
      CountSerDeSQL(row.getInt(0), 1 + row.getInt(1), row.getDouble(2))
    case u => throw new Exception(s"failed to deserialize: $u")
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: CountSerDeUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[CountSerDeUDT].getName.hashCode()
}

case object CountSerDeUDT extends CountSerDeUDT

case object CountSerDeUDIA extends UserDefinedImperativeAggregator[CountSerDeSQL] with Logging {
  def inputSchema: StructType = StructType(StructField("x", DoubleType) :: Nil)
  def resultType: DataType = CountSerDeUDT
  def deterministic: Boolean = true
  def initial: CountSerDeSQL = CountSerDeSQL(0, 0, 0)
  def update(agg: CountSerDeSQL, input: Row): CountSerDeSQL =
    agg.copy(sum = agg.sum + input.getDouble(0))
  def merge(agg1: CountSerDeSQL, agg2: CountSerDeSQL): CountSerDeSQL =
    CountSerDeSQL(agg1.nSer + agg2.nSer, agg1.nDeSer + agg2.nDeSer, agg1.sum + agg2.sum)
  def evaluate(agg: CountSerDeSQL): Any = agg

  import java.io._
  // scalastyle:off classforname
  class ObjectInputStreamWithCustomClassLoader(
    inputStream: InputStream) extends ObjectInputStream(inputStream) {
    override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
      try { Class.forName(desc.getName, false, getClass.getClassLoader) }
      catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
    }
  }
  // scalastyle:off classforname

  def serialize(agg: CountSerDeSQL): Array[Byte] = {
    val bufout = new ByteArrayOutputStream()
    val obout = new ObjectOutputStream(bufout)
    obout.writeObject(agg.copy(nSer = agg.nSer + 1))
    bufout.toByteArray
  }
  def deserialize(data: Array[Byte]): CountSerDeSQL = {
    val bufin = new ByteArrayInputStream(data)
    val obin = new ObjectInputStreamWithCustomClassLoader(bufin)

    val agg = obin.readObject().asInstanceOf[CountSerDeSQL]
    agg.copy(nDeSer = agg.nDeSer + 1)
  }
}

case object CountSerDeUDAF extends UserDefinedAggregateFunction {
  def deterministic: Boolean = true

  def inputSchema: StructType = StructType(StructField("x", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("count-ser-de", CountSerDeUDT) :: Nil)

  def dataType: DataType = CountSerDeUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) = CountSerDeSQL(0, 0, 0)
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    val sql = buf.getAs[CountSerDeSQL](0)
    buf(0) = sql.copy(sum = sql.sum + input.getDouble(0))
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    val sql1 = buf1.getAs[CountSerDeSQL](0)
    val sql2 = buf2.getAs[CountSerDeSQL](0)
    buf1(0) = CountSerDeSQL(sql1.nSer + sql2.nSer, sql1.nDeSer + sql2.nDeSer, sql1.sum + sql2.sum)
  }

  def evaluate(buf: Row): Any = buf.getAs[CountSerDeSQL](0)
}
