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

package org.apache.spark.deploy.history

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerNodeBlacklisted, SparkListenerNodeUnblacklisted}
import org.apache.spark.util.JsonProtocol

object EventLogTestHelper {
  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis

  /**
   * Get a SparkConf with event logging enabled. It doesn't enable rolling event logs, so caller
   * should set it manually.
   */
  def getLoggingConf(logDir: Path, compressionCodec: Option[String] = None): SparkConf = {
    val conf = new SparkConf
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_BLOCK_UPDATES, true)
    conf.set(EVENT_LOG_TESTING, true)
    conf.set(EVENT_LOG_DIR, logDir.toString)
    compressionCodec.foreach { codec =>
      conf.set(EVENT_LOG_COMPRESS, true)
      conf.set(EVENT_LOG_COMPRESSION_CODEC, codec)
    }
    conf.set(EVENT_LOG_STAGE_EXECUTOR_METRICS, true)
    conf
  }

  def writeTestEvents(
      writer: EventLogFileWriter,
      eventStr: String,
      desiredSize: Long): Seq[String] = {
    val stringLen = eventStr.getBytes(StandardCharsets.UTF_8).length
    val repeatCount = Math.floor(desiredSize / stringLen).toInt
    (0 until repeatCount).map { _ =>
      writer.writeEvent(eventStr, flushLogger = true)
      eventStr
    }
  }

  def writeEventLogFile(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      dir: File,
      idx: Int,
      events: Seq[SparkListenerEvent]): String = {
    // to simplify the code, we don't concern about file name being matched with the naming rule
    // of event log file
    val writer = new SingleEventLogFileWriter(s"app$idx", None, dir.toURI, sparkConf, hadoopConf)
    writer.start()
    events.foreach { event => writer.writeEvent(convertEvent(event), flushLogger = true) }
    writer.stop()
    writer.logPath
  }

  def convertEvent(event: SparkListenerEvent): String = {
    compact(render(JsonProtocol.sparkEventToJson(event)))
  }

  class TestEventFilter1 extends EventFilter {
    override def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = {
      Some(true)
    }

    override def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = {
      Some(true)
    }

    override def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = {
      Some(false)
    }
  }

  class TestEventFilter2 extends EventFilter {
    override def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = {
      Some(true)
    }

    override def filterEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Option[Boolean] = {
      Some(true)
    }

    override def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = {
      Some(false)
    }

    override def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = {
      Some(false)
    }

    override def filterNodeBlacklisted(event: SparkListenerNodeBlacklisted): Option[Boolean] = {
      Some(true)
    }

    override def filterNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Option[Boolean] = {
      Some(false)
    }
  }
}
