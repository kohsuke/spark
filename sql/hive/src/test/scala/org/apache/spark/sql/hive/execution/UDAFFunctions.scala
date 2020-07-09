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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

class ScalaAggregateFunction(schema: StructType) extends UserDefinedAggregateFunction {

  def inputSchema: StructType = schema

  def bufferSchema: StructType = schema

  def dataType: DataType = schema

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    (0 until schema.length).foreach { i =>
      buffer.update(i, null)
    }
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0) && input.getInt(0) == 50) {
      (0 until schema.length).foreach { i =>
        buffer.update(i, input.get(i))
      }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0) && buffer2.getInt(0) == 50) {
      (0 until schema.length).foreach { i =>
        buffer1.update(i, buffer2.get(i))
      }
    }
  }

  def evaluate(buffer: Row): Any = {
    Row.fromSeq(buffer.toSeq)
  }
}

class ScalaAggregateFunctionWithoutInputSchema extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(Nil)

  def bufferSchema: StructType = StructType(StructField("value", LongType) :: Nil)

  def dataType: DataType = LongType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0L)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, input.getAs[Seq[Row]](0).map(_.getAs[Int]("v")).sum + buffer.getLong(0))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
  }

  def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

class LongProductSum extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType()
    .add("a", LongType)
    .add("b", LongType)

  def bufferSchema: StructType = new StructType()
    .add("product", LongType)

  def dataType: DataType = LongType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!(input.isNullAt(0) || input.isNullAt(1))) {
      buffer(0) = buffer.getLong(0) + input.getLong(0) * input.getLong(1)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  def evaluate(buffer: Row): Any =
    buffer.getLong(0)
}

