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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData}
import org.apache.spark.unsafe.Platform

class UnsafeMapSuite extends SparkFunSuite {

  val unsafeMapData = {
    val offset = 32
    val keyArraySize = 256
    val baseObject = new Array[Byte](1024)
    Platform.putLong(baseObject, offset, keyArraySize)

    val unsafeMap = new UnsafeMapData
    Platform.putLong(baseObject, offset + 8, 1)
    val keyArray = new UnsafeArrayData()
    keyArray.pointTo(baseObject, offset + 8, keyArraySize)
    keyArray.setLong(0, 19285)

    val valueArray = new UnsafeArrayData()
    Platform.putLong(baseObject, offset + 8 + keyArray.getSizeInBytes, 1)
    valueArray.pointTo(baseObject, offset + 8 + keyArray.getSizeInBytes, keyArraySize)
    valueArray.setLong(0, 19286)
    unsafeMap.pointTo(baseObject, offset, baseObject.length)
    unsafeMap
  }

  test("unsafe java serialization") {
    val ser = new JavaSerializer(new SparkConf).newInstance()
    val mapDataSer = ser.deserialize[UnsafeMapData](ser.serialize(unsafeMapData))
    assert(mapDataSer.numElements() == 1)
    assert(mapDataSer.keyArray().getInt(0) == 19285)
    assert(mapDataSer.valueArray().getInt(0) == 19286)
    assert(mapDataSer.getBaseObject.asInstanceOf[Array[Byte]].length == 1024)
  }

  test("unsafe Kryo serialization") {
    val ser = new KryoSerializer(new SparkConf).newInstance()
    val mapDataSer = ser.deserialize[UnsafeMapData](ser.serialize(unsafeMapData))
    assert(mapDataSer.numElements() == 1)
    assert(mapDataSer.keyArray().getInt(0) == 19285)
    assert(mapDataSer.valueArray().getInt(0) == 19286)
    assert(mapDataSer.getBaseObject.asInstanceOf[Array[Byte]].length == 1024)

  test("UnsafeMap from two UnsafeArray") {
    val keys = Array[String]("apache", "spark", "with", "kafka")
    val values = Array[Array[Byte]](
      Array[Byte](1.toByte, 2.toByte), Array[Byte](3.toByte, 4.toByte, 5.toByte),
      Array[Byte](6.toByte), Array[Byte]()
    )
    val unsafeKeyData = UnsafeArrayData.fromStringArray(keys)
    val unsafeValueData = UnsafeArrayData.fromBinaryArray(values)
    val unsafeMapData = UnsafeMapData.of(unsafeKeyData, unsafeValueData)

    assert(unsafeMapData.numElements() == 4)
    assert(unsafeMapData.keyArray().getUTF8String(0).toString == "apache")
    assert(unsafeMapData.keyArray().getUTF8String(1).toString == "spark")
    assert(unsafeMapData.keyArray().getUTF8String(2).toString == "with")
    assert(unsafeMapData.keyArray().getUTF8String(3).toString == "kafka")
    assert(unsafeMapData.valueArray().getBinary(0).sameElements(Array[Byte](1.toByte, 2.toByte)))
    assert(unsafeMapData.valueArray().getBinary(1)
      .sameElements(Array[Byte](3.toByte, 4.toByte, 5.toByte)))
    assert(unsafeMapData.valueArray().getBinary(2).sameElements(Array[Byte](6.toByte)))
    assert(unsafeMapData.valueArray().getBinary(3) == null)
  }
}
