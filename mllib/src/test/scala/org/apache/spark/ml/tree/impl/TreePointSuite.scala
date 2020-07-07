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

package org.apache.spark.ml.tree.impl

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.util.MLUtils
import org.apache.spark.serializer.KryoSerializer

class TreePointSuite extends SparkFunSuite {
  test("Kryo class register") {
    val conf = new SparkConf(false)
    MLUtils.registerKryoClasses(conf)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val values = Array(1, 2, 3)

    {
      val point = new TreePoint[Int](1.0, values, 1.0)
      val point2 = ser.deserialize[TreePoint[Int]](ser.serialize(point))
      assert(point.label === point2.label)
      assert(point.binnedFeatures === point2.binnedFeatures)
    }

    {
      val point = new TreePoint[Short](1.0, values.map(_.toShort), 1.0)
      val point2 = ser.deserialize[TreePoint[Short]](ser.serialize(point))
      assert(point.label === point2.label)
      assert(point.binnedFeatures === point2.binnedFeatures)
    }

    {
      val point = new TreePoint[Byte](1.0, values.map(_.toByte), 1.0)
      val point2 = ser.deserialize[TreePoint[Byte]](ser.serialize(point))
      assert(point.label === point2.label)
      assert(point.binnedFeatures === point2.binnedFeatures)
    }
  }
}
