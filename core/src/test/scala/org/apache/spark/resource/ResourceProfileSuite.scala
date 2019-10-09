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

package org.apache.spark.resource

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceUtils._

class ResourceProfileSuite extends SparkFunSuite {

  override def afterEach() {
    try {
      ResourceProfile.resetDefaultProfile(new SparkConf)
    } finally {
      super.afterEach()
    }
  }

  test("Default ResourceProfile") {
    val rprof = ResourceProfile.getOrCreateDefaultProfile(new SparkConf)
    assert(rprof.getId === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.getExecutorResources.size === 2,
      "Executor resources should contain cores and memory by default")
    assert(rprof.getExecutorResources(ResourceProfile.CORES).amount === 1,
      s"Executor resources should have 1 core")
    assert(rprof.getExecutorResources(ResourceProfile.MEMORY).amount === 1024,
      s"Executor resources should have 1024 memory")
    assert(rprof.getTaskResources.size === 1,
      "Task resources should just contain cpus by default")
    assert(rprof.getTaskResources(ResourceProfile.CPUS).amount === 1,
      s"Task resources should have 1 cpu")
  }

  test("Default ResourceProfile with app level resources specified") {
    val conf = new SparkConf
    conf.set("spark.task.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.amount", "1")
    conf.set(s"$SPARK_EXECUTOR_PREFIX.resource.gpu.discoveryScript", "nameOfScript")
    val all = conf.getAllWithPrefix(s"$SPARK_EXECUTOR_PREFIX.$RESOURCE_DOT")
    logInfo("tests ids are: " + all.mkString(","))
    val rprof = ResourceProfile.getOrCreateDefaultProfile(conf)
    assert(rprof.getId === ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val execResources = rprof.getExecutorResources
    assert(execResources.size === 3,
      "Executor resources should contain cores, memory, and gpu " + execResources)
    assert(rprof.getTaskResources.size === 2,
      "Task resources should just contain cpus and gpu")
    assert(execResources.contains("resource.gpu"), "Executor resources should have gpu")
    assert(rprof.getTaskResources.contains("resource.gpu"), "Task resources should have gpu")
  }

  test("Create ResourceProfile") {
    val rprof = new ResourceProfile()
    assert(rprof.getId > ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rprof.getExecutorResources === Map.empty)
    assert(rprof.getTaskResources === Map.empty)

    val taskReq = new TaskResourceRequest("resource.gpu", 1)
    val execReq = new ExecutorResourceRequest("resource.gpu", 2, Some("myscript"), None)
    rprof.require(taskReq).require(execReq)

    assert(rprof.getExecutorResources.size === 1)
    assert(rprof.getExecutorResources.contains("resource.gpu"),
      "Executor resources should have gpu")
    assert(rprof.getTaskResources.size === 1)
    assert(rprof.getTaskResources.contains("resource.gpu"), "Task resources should have gpu")

    val cpuTaskReq = new TaskResourceRequest(ResourceProfile.CPUS, 1)
    val coresExecReq = new ExecutorResourceRequest(ResourceProfile.CORES, 2)
    val memExecReq = new ExecutorResourceRequest(ResourceProfile.MEMORY, 4096)
    val omemExecReq = new ExecutorResourceRequest(ResourceProfile.OVERHEAD_MEM, 2048)
    val pysparkMemExecReq = new ExecutorResourceRequest(ResourceProfile.PYSPARK_MEM, 1024)

    rprof.require(cpuTaskReq)
    rprof.require(coresExecReq)
    rprof.require(memExecReq)
    rprof.require(omemExecReq)
    rprof.require(pysparkMemExecReq)

    assert(rprof.getExecutorResources.size === 5)
    assert(rprof.getExecutorResources(ResourceProfile.CORES).amount === 2,
      s"Executor resources should have 2 cores")
    assert(rprof.getExecutorResources(ResourceProfile.MEMORY).amount === 4096,
      s"Executor resources should have 4096 memory")
    assert(rprof.getExecutorResources(ResourceProfile.OVERHEAD_MEM).amount === 2048,
      s"Executor resources should have 2048 overhead memory")
    assert(rprof.getExecutorResources(ResourceProfile.PYSPARK_MEM).amount === 1024,
      s"Executor resources should have 1024 pyspark memory")

    assert(rprof.getTaskResources.size === 2)
    assert(rprof.getTaskResources("cpus").amount === 1, "Task resources should have cpu")

    val error = intercept[IllegalArgumentException] {
      rprof.require(new ExecutorResourceRequest("bogusResource", 1))
    }.getMessage()
    assert(error.contains("Executor resource not allowed"))

    val taskError = intercept[IllegalArgumentException] {
      rprof.require(new TaskResourceRequest("bogusTaskResource", 1))
    }.getMessage()
    assert(taskError.contains("Task resource not allowed"))
  }
}