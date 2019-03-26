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

package org.apache.spark.sql.kafka010

import scala.collection.JavaConverters.asScalaIterator
import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.common.header.Headers

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utilities for converting Kafka related objects to and from json.
 */
private object KafkaUtils {

  /**
   * Returns required memory sizes (bytes) to convert given blob array into unsafe format
   * ([[UnsafeArrayData]]), in (header size, value region size, variable length region size) scheme.
   */
  private def calculateSizes(blobs: Seq[Array[Byte]]) = {
    if (blobs == null) {
      (0, 0, 0)
    } else {
      val headerSize = UnsafeArrayData.calculateHeaderPortionInBytes(blobs.length)
      val valueRegionSize = blobs.length * 8
      val variableLengthRegionSizeInLong = blobs.map(
        blob => {
          if (blob == null) {
            0L
          } else {
            (blob.length + 7) / 8
          }
        }
      ).sum

      if (variableLengthRegionSizeInLong == 0) {
        (0, 0, 0)
      } else {
        val totalSizeInLong = (headerSize + valueRegionSize) / 8 + variableLengthRegionSizeInLong
        if (totalSizeInLong > Integer.MAX_VALUE / 8) {
          throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
            "it's too big.")
        }

        (headerSize, valueRegionSize, variableLengthRegionSizeInLong.toInt * 8)
      }
    }
  }

  /**
   * Creates [[UnsafeMapData]] from given Kafka [[Headers]].
   */
  def toUnsafeMapData(headers: Headers) : UnsafeMapData = {
    val unsafeMapData = new UnsafeMapData
    var count = 0
    val keyBlobs = new ArrayBuffer[Array[Byte]]
    val valueBlobs = new ArrayBuffer[Array[Byte]]
    for (header <- asScalaIterator(headers.iterator)) {
      count += 1
      val utf8String = UTF8String.fromString(header.key)
      val blob = if (utf8String != null) {
        utf8String.getBytes
      } else {
        null
      }
      keyBlobs.append(blob)
      valueBlobs.append(header.value)
    }
    val (keyHeaderSize, keyValueRegionSize, keyVariableLengthRegionSize) = calculateSizes(keyBlobs)
    val keyTotalSize = keyHeaderSize + keyValueRegionSize + keyVariableLengthRegionSize
    if (count == 0 || keyTotalSize == 0) {
      val data = Array[Byte](8)
      Platform.putLong(data, Platform.BYTE_ARRAY_OFFSET, 0)
      unsafeMapData.pointTo(data, Platform.BYTE_ARRAY_OFFSET, 8)
    } else {
      val (valueHeaderSize, valueValueRegionSize, valueVariableLengthRegionSize) =
        calculateSizes(valueBlobs)
      val valueTotalSize = valueHeaderSize + valueValueRegionSize + valueVariableLengthRegionSize

      val data = new Array[Byte](8 + keyTotalSize + valueTotalSize)

      // put key size
      Platform.putLong(data, Platform.BYTE_ARRAY_OFFSET, keyTotalSize)
      Platform.putLong(data, Platform.BYTE_ARRAY_OFFSET + 8, count)
      Platform.putLong(data, Platform.BYTE_ARRAY_OFFSET + 8 + keyTotalSize, count)

      // put keys
      val unsafeKeyArray = new UnsafeArrayData
      unsafeKeyArray.pointTo(data, Platform.BYTE_ARRAY_OFFSET + 8, keyTotalSize)
      var offset = keyHeaderSize + keyValueRegionSize
      for (i <- 0 until count) {
        val blob = keyBlobs(i)
        if (blob == null || blob.length == 0) {
          // null object
          unsafeKeyArray.setNullAt(i)
        } else {
          // offset and size
          val size = blob.length
          val offsetAndSize = (offset.toLong << 32) | size
          unsafeKeyArray.setLong(i, offsetAndSize)
          // copy contents
          Platform.copyMemory(blob, Platform.BYTE_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 8 + offset, size)
          // update offset
          offset += ((size + 7) / 8) * 8
        }
      }

      // put values
      val unsafeValueArray = new UnsafeArrayData
      unsafeValueArray.pointTo(data, Platform.BYTE_ARRAY_OFFSET + 8 + keyTotalSize, valueTotalSize)
      offset = valueHeaderSize + valueValueRegionSize
      for (i <- 0 until count) {
        val blob = valueBlobs(i)
        if (blob == null || blob.length == 0) {
          // null object
          unsafeValueArray.setNullAt(i)
        } else {
          // offset and size
          val size = blob.length
          val offsetAndSize = (offset.toLong << 32) | size
          unsafeValueArray.setLong(i, offsetAndSize)
          // copy contents
          Platform.copyMemory(blob, Platform.BYTE_ARRAY_OFFSET, data,
            Platform.BYTE_ARRAY_OFFSET + 8 + keyTotalSize + offset, size)
          // update offset
          offset += ((size + 7) / 8) * 8
        }
      }

      unsafeMapData.pointTo(data, Platform.BYTE_ARRAY_OFFSET, 8 + keyTotalSize + valueTotalSize)
    }
    unsafeMapData
  }
}
