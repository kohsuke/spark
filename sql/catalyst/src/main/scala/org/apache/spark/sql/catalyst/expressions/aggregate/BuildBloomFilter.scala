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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.{Double => JDouble, Float => JFloat}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BinaryType, DoubleType, FloatType, StringType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.util.sketch.BloomFilter

case class BuildBloomFilter(
    child: Expression,
    expectedNumItems: Long,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[BloomFilter] with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(BuildBloomFilter.buildBloomFilterTypes: _*))
  }

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = BinaryType

  override def createAggregationBuffer(): BloomFilter = {
    BloomFilter.create(expectedNumItems)
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      child.dataType match {
        case _: IntegralType =>
          buffer.putLong(value.asInstanceOf[Number].longValue())
        case DateType | TimestampType =>
          buffer.putLong(value.asInstanceOf[Number].longValue())
        case FloatType =>
          buffer.putLong(JFloat.floatToIntBits(value.asInstanceOf[Float]).toLong)
        case DoubleType =>
          buffer.putLong(JDouble.doubleToLongBits(value.asInstanceOf[Double]))
        case StringType =>
          buffer.putBinary(value.asInstanceOf[UTF8String].getBytes)
        case BinaryType =>
          buffer.putBinary(value.asInstanceOf[Array[Byte]])
        case _: DecimalType =>
          buffer.putBinary(value.asInstanceOf[Decimal].toJavaBigDecimal.unscaledValue().toByteArray)
      }
    }
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(input)
    buffer
  }

  override def eval(buffer: BloomFilter): Any = serialize(buffer)

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream) { out =>
      buffer.writeTo(out)
      out.toByteArray
    }
  }

  override def deserialize(storageFormat: Array[Byte]): BloomFilter = {
    Utils.tryWithResource(new ByteArrayInputStream(storageFormat)) { in =>
      BloomFilter.readFrom(in)
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): BuildBloomFilter =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): BuildBloomFilter =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "build_bloom_filter"
}

object BuildBloomFilter {
  val buildBloomFilterTypes: Seq[AbstractDataType] =
    Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType, DateType, TimestampType,
      StringType, BinaryType)

  def isSupportBuildBloomFilterType(dataType: DataType): Boolean = {
    dataType match {
      case _: DecimalType => true
      case other => buildBloomFilterTypes.contains(other)
    }
  }

  def isSupportBuildBloomFilter(expression: Expression): Boolean = {
    isSupportBuildBloomFilterType(expression.dataType)
  }
}
