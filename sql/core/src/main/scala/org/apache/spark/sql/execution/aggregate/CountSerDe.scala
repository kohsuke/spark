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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[CountSerDeUDT])
case class CountSerDeSQL(nSer: Int, nDeSer: Int)

class CountSerDeUDT extends UserDefinedType[CountSerDeSQL] {
  def userClass: Class[CountSerDeSQL] = classOf[CountSerDeSQL]

  override def typeName: String = "count-ser-de"

  private[spark] override def asNullable: CountSerDeUDT = this

  def sqlType: DataType = StructType(
    StructField("nSer", IntegerType, false) ::
    StructField("nDeSer", IntegerType, false) ::
    Nil)

  def serialize(sql: CountSerDeSQL): Any = {
    val row = new GenericInternalRow(2)
    row.setInt(0, 1 + sql.nSer)
    row.setInt(1, sql.nDeSer)
    row
  }

  def deserialize(any: Any): CountSerDeSQL = any match {
    case row: InternalRow if (row.numFields == 2) =>
      CountSerDeSQL(row.getInt(0), 1 + row.getInt(1))
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

case object CountSerDeUDAF extends UserDefinedAggregateFunction {
  def deterministic: Boolean = true

  def inputSchema: StructType = StructType(StructField("x", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("count-ser-de", CountSerDeUDT) :: Nil)

  def dataType: DataType = CountSerDeUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) = CountSerDeSQL(0, 0)
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    val sql = buf.getAs[CountSerDeSQL](0)
    buf(0) = sql
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    val sql1 = buf1.getAs[CountSerDeSQL](0)
    val sql2 = buf2.getAs[CountSerDeSQL](0)
    buf1(0) = CountSerDeSQL(sql1.nSer + sql2.nSer, sql1.nDeSer + sql2.nDeSer)
  }

  def evaluate(buf: Row): Any = buf.getAs[CountSerDeSQL](0)
}
