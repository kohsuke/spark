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
package org.apache.spark

trait SparkContextHelper {

  def withSparkContext(pairs: (String, String)*)(testFun: SparkContext => Any): Unit = {
    setupSparkContext(2, pairs: _*)(testFun)
  }

  def withSparkContext(numCores: Int, pairs: (String, String)*)
       (testFun: SparkContext => Any): Unit = {
    setupSparkContext(numCores, pairs: _*)(testFun)
  }

  /** Sets all configurations specified in `pairs`, calls `init`, and then calls `testFun` */
  private def setupSparkContext(numCores: Int, pairs: (String, String)*)
      (testFun: SparkContext => Any): Unit = {
    val conf = new SparkConf()
    pairs.foreach(kv => conf.set(kv._1, kv._2))
    val sc = new SparkContext(s"local[$numCores]", "DAGSchedulerSuite", conf)
    testFun(sc)
  }
}
