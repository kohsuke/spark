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

package org.apache.spark.deploy

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages.IsWorkerReadyToStop
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

/**
 * Testing class that creates a Spark standalone process in-cluster (that is, running the
 * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
 * by the Workers still run in separate JVMs. This can be used to test distributed operation and
 * fault recovery without spinning up a lot of processes.
 */
private[spark]
class LocalSparkCluster(
    numWorkers: Int,
    coresPerWorker: Int,
    memoryPerWorker: Int,
    conf: SparkConf)
  extends Logging {

  private val localHostname = Utils.localHostName()
  private val masterRpcEnvs = ArrayBuffer[RpcEnv]()
  private val workerRpcEnvs = ArrayBuffer[RpcEnv]()
  private val workerRefs = ArrayBuffer[RpcEndpointRef]()
  // exposed for testing
  var masterWebUIPort = -1

  def start(): Array[String] = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    // Disable REST server on Master in this mode unless otherwise specified
    val _conf = conf.clone()
      .setIfMissing(config.MASTER_REST_SERVER_ENABLED, false)
      .set(config.SHUFFLE_SERVICE_ENABLED, false)

    /* Start the Master */
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
    masterWebUIPort = webUiPort
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    val masters = Array(masterUrl)

    /* Start the Workers */
    for (workerNum <- 1 to numWorkers) {
      val (workerEnv, workerRef) = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0,
        coresPerWorker, memoryPerWorker, masters, null, Some(workerNum), _conf,
        conf.get(config.Worker.SPARK_WORKER_RESOURCE_FILE))
      workerRpcEnvs += workerEnv
      workerRefs += workerRef
    }

    masters
  }

  def stop(): Unit = {
    logInfo("Shutting down local Spark cluster.")
    // SPARK-31922: make sure all the workers have handled the messages(`KillExecutor`,
    // `ApplicationFinished`) from the Master before we shutdown the workers' rpcEnvs.
    // Otherwise, we could hit "RpcEnv already stopped" error.
    var busyWorkers = workerRefs
    while (busyWorkers.nonEmpty) {
      Thread.sleep(100)
      busyWorkers = busyWorkers.filter(_.askSync[Boolean](IsWorkerReadyToStop))
    }

    // Stop the workers before the master so they don't get upset that it disconnected
    Seq(workerRpcEnvs, masterRpcEnvs).foreach { rpcEnvArr =>
      rpcEnvArr.foreach { rpcEnv => Utils.tryLog {
        rpcEnv.shutdown()
        rpcEnv.awaitTermination()
      }}
      rpcEnvArr.clear()
    }
  }
}
