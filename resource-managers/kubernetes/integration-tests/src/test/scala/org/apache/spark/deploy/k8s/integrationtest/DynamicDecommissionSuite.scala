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
package org.apache.spark.deploy.k8s.integrationtest

import org.apache.spark.internal.config
import org.apache.spark.internal.config.Worker

private[spark] trait DynamicDecommissionSuite { k8sSuite: KubernetesSuite =>

  import DecommissionSuite._
  import KubernetesSuite.k8sTestTag

  test("Test dynamic decommissioning", k8sTestTag) {
    sparkAppConf
      .set(Worker.WORKER_DECOMMISSION_ENABLED.key, "true")
      .set("spark.kubernetes.pyspark.pythonVersion", "3")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_RDD_DECOMMISSION_ENABLED.key, "true")
      // Ensure we have somewhere to migrate our data too
      .set("spark.dynamicAllocation.initialExecutors", "3")
      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
      .set("spark.storage.decommission.replicationReattemptInterval", "1")
      // Set the idle timeouts really low
      .set("spark.dynamicAllocation.executorIdleTimeout", "10s")
      // We only want to decommission one node
      .set("spark.dynamicAllocation.minExecutors", "2")


    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_DECOMISSIONING,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "decommissioning executor",
        "Final accumulator value is: 100"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None)
  }
}

private[spark] object DecommissionSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "dynamic_decommissioning.py"
}
