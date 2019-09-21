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

import java.io.{File, FileOutputStream, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper._
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils


abstract class EventLogFileWritersSuite extends SparkFunSuite with LocalSparkContext
  with BeforeAndAfter {

  protected val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  protected var testDir: File = _
  protected var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir(namePrefix = s"event log")
    testDir.deleteOnExit()
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("create EventLogFileWriter with enable/disable rolling") {
    def buildWriterAndVerify(conf: SparkConf, expectedClazz: Class[_]): Unit = {
      val writer = EventLogFileWriter(
        getUniqueApplicationId, None, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))
      val writerClazz = writer.getClass
      assert(expectedClazz === writerClazz,
        s"default file writer should be $expectedClazz, but $writerClazz")
    }

    val conf = new SparkConf
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_DIR, testDir.toString)

    // default config
    buildWriterAndVerify(conf, classOf[SingleEventLogFileWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, true)
    buildWriterAndVerify(conf, classOf[RollingEventLogFilesWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, false)
    buildWriterAndVerify(conf, classOf[SingleEventLogFileWriter])
  }

  val allCodecs = Seq(None) ++
    CompressionCodec.ALL_COMPRESSION_CODECS.map(c => Some(CompressionCodec.getShortName(c)))

  allCodecs.foreach { codecShortName =>
    test(s"initialize, write, stop - with codec $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      // snappy stream throws exception on empty stream, so we should provide some data to test.
      val dummyData = Seq("dummy1", "dummy2", "dummy3")
      dummyData.foreach(writer.writeEvent(_, flushLogger = true))

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName,
        isCompleted = false, dummyData)

      writer.stop()

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName,
        isCompleted = true, dummyData)
    }
  }

  test("spark.eventLog.compression.codec overrides spark.io.compression.codec") {
    val conf = new SparkConf
    conf.set(EVENT_LOG_COMPRESS, true)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

    val appId = "test"
    val appAttemptId = None

    // The default value is `spark.io.compression.codec`.
    val writer = createWriter(appId, appAttemptId, testDirPath.toUri, conf, hadoopConf)
    assert(writer.compressionCodecName.contains("lz4"))

    // `spark.eventLog.compression.codec` overrides `spark.io.compression.codec`.
    conf.set(EVENT_LOG_COMPRESSION_CODEC, "zstd")
    val writer2 = createWriter(appId, appAttemptId, testDirPath.toUri, conf, hadoopConf)
    assert(writer2.compressionCodecName.contains("zstd"))
  }

  protected def readLinesFromEventLogFile(log: Path, fs: FileSystem): List[String] = {
    val logDataStream = EventLogFileReader.openEventLog(log, fs)
    try {
      Source.fromInputStream(logDataStream).getLines().toList
    } finally {
      logDataStream.close()
    }
  }

  protected def createWriter(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter

  protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String] = Seq.empty): Unit

  protected def skipVerifyEventLogFile(
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Boolean = {
    // Spark initializes LZ4BlockOutputStream with syncFlush=false, so we can't force
    // pending bytes to be flushed. It's only guaranteed when stream is closed, so
    // we only check for lz4 when isCompleted = true.

    // zstd seems to have issue in reading while write stream is in progress of writing
    !isCompleted &&
      (compressionCodecShortName.contains("lz4") || compressionCodecShortName.contains("zstd"))
  }
}

class SingleEventLogFileWriterSuite extends EventLogFileWritersSuite {

  test("Log overwriting") {
    val appId = "test"
    val appAttemptId = None
    val logUri = SingleEventLogFileWriter.getLogPath(testDir.toURI, appId, appAttemptId)

    val conf = getLoggingConf(testDirPath)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val writer = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)

    val logPath = new Path(logUri).toUri.getPath
    writer.start()

    val dummyData = Seq("dummy1", "dummy2", "dummy3")
    dummyData.foreach(writer.writeEvent(_, flushLogger = true))

    // Create file before writing the event log
    new FileOutputStream(new File(logPath)).close()
    // Expected IOException, since we haven't enabled log overwrite.
    intercept[IOException] { writer.stop() }

    // Try again, but enable overwriting.
    conf.set(EVENT_LOG_OVERWRITE, true)
    val writer2 = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)
    writer2.start()
    dummyData.foreach(writer2.writeEvent(_, flushLogger = true))
    writer2.stop()
  }

  test("Event log name") {
    val baseDirUri = Utils.resolveURI("/base-dir")
    // without compression
    assert(s"${baseDirUri.toString}/app1" === SingleEventLogFileWriter.getLogPath(
      baseDirUri, "app1", None, None))
    // with compression
    assert(s"${baseDirUri.toString}/app1.lzf" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri, "app1", None, Some("lzf")))
    // illegal characters in app ID
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None, None))
    // illegal characters in app ID with compression
    assert(s"${baseDirUri.toString}/a-fine-mind_dollar_bills__1.lz4" ===
      SingleEventLogFileWriter.getLogPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None, Some("lz4")))
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String]): Unit = {
    // read single event log file
    val logPath = SingleEventLogFileWriter.getLogPath(logBaseDir, appId, appAttemptId,
      compressionCodecShortName)

    val finalLogPath = if (!isCompleted) {
      new Path(logPath + EventLogFileWriter.IN_PROGRESS)
    } else {
      new Path(logPath)
    }

    assert(fileSystem.exists(finalLogPath) && fileSystem.isFile(finalLogPath))
    if (!skipVerifyEventLogFile(compressionCodecShortName, isCompleted)) {
      assert(expectedLines === readLinesFromEventLogFile(finalLogPath, fileSystem))
    }
  }
}

class RollingEventLogFilesWriterSuite extends EventLogFileWritersSuite {
  import RollingEventLogFilesWriter._

  test("Event log names") {
    val baseDirUri = Utils.resolveURI("/base-dir")
    val appId = "app1"
    val appAttemptId = None

    // happy case with app ID
    val logDir = RollingEventLogFilesWriter.getAppEventLogDirPath(baseDirUri, appId, None)
    assert(s"${baseDirUri.toString}/eventlog_v2_app1" === logDir.toString)

    // appstatus: inprogress or completed
    assert(s"$logDir/appstatus_app1.inprogress" ===
      RollingEventLogFilesWriter.getAppStatusFilePath(logDir, appId, appAttemptId,
        inProgress = true).toString)
    assert(s"$logDir/appstatus_app1" ===
      RollingEventLogFilesWriter.getAppStatusFilePath(logDir, appId, appAttemptId,
        inProgress = false).toString)

    // without compression
    assert(s"$logDir/events_1_app1" ===
      RollingEventLogFilesWriter.getEventLogFilePath(logDir, appId, appAttemptId, 1, None).toString)

    // with compression
    assert(s"$logDir/events_1_app1.lzf" ===
      RollingEventLogFilesWriter.getEventLogFilePath(logDir, appId, appAttemptId,
        1, Some("lzf")).toString)

    // illegal characters in app ID
    assert(s"${baseDirUri.toString}/eventlog_v2_a-fine-mind_dollar_bills__1" ===
      RollingEventLogFilesWriter.getAppEventLogDirPath(baseDirUri,
        "a fine:mind$dollar{bills}.1", None).toString)
  }

  test("Log overwriting") {
    val appId = "test"
    val appAttemptId = None
    val logDirPath = RollingEventLogFilesWriter.getAppEventLogDirPath(testDir.toURI, appId,
      appAttemptId)

    val conf = getLoggingConf(testDirPath)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val writer = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)

    val logPath = logDirPath.toUri.getPath

    // Create file before writing the event log directory
    // it doesn't matter whether the existing one is file or directory
    new FileOutputStream(new File(logPath)).close()

    // Expected IOException, since we haven't enabled log overwrite.
    // Note that the place IOException is thrown is different from single event log file.
    intercept[IOException] { writer.start() }

    // Try again, but enable overwriting.
    conf.set(EVENT_LOG_OVERWRITE, true)

    val writer2 = createWriter(appId, appAttemptId, testDir.toURI, conf, hadoopConf)
    writer2.start()
    val dummyData = Seq("dummy1", "dummy2", "dummy3")
    dummyData.foreach(writer2.writeEvent(_, flushLogger = true))
    writer2.stop()
  }

  allCodecs.foreach { codecShortName =>
    test(s"rolling event log files - codec $codecShortName") {
      def assertEventLogFilesSequence(
          eventLogFiles: Seq[FileStatus],
          expectedLastSequence: Int,
          expectedMaxSizeBytes: Long): Unit = {
        assert(eventLogFiles.forall(f => f.getLen < expectedMaxSizeBytes))
        assert((1 to expectedLastSequence) ===
          eventLogFiles.map(f => getSequence(f.getPath.getName)))
      }

      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      conf.set(EVENT_LOG_ENABLE_ROLLING, true)
      conf.set(EVENT_LOG_ROLLED_EVENT_LOG_MAX_FILE_SIZE.key, "1k")

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      val dummyString = "dummy"
      val dummyStringBytesLen = dummyString.getBytes(StandardCharsets.UTF_8).length
      val expectedLines = mutable.ArrayBuffer[String]()

      // write log more than 2k (intended to roll over to 3 files)
      val repeatCount = Math.floor((1024 * 2) / dummyStringBytesLen).toInt
      (0 until repeatCount).foreach { _ =>
        expectedLines.append(dummyString)
        writer.writeEvent(dummyString, flushLogger = true)
      }

      val logDirPath = getAppEventLogDirPath(testDirPath.toUri, appId, attemptId)

      val eventLogFiles = listEventLogFiles(logDirPath)
      assertEventLogFilesSequence(eventLogFiles, 3, 1024 * 1024)

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri,
        codecShortName, isCompleted = false, expectedLines)

      writer.stop()

      val eventLogFiles2 = listEventLogFiles(logDirPath)
      assertEventLogFilesSequence(eventLogFiles2, 3, 1024 * 1024)

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri,
        codecShortName, isCompleted = true, expectedLines)
    }
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String]): Unit = {
    val logDirPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

    assert(fileSystem.exists(logDirPath) && fileSystem.isDirectory(logDirPath))

    val appStatusFile = getAppStatusFilePath(logDirPath, appId, appAttemptId, !isCompleted)
    assert(fileSystem.exists(appStatusFile) && fileSystem.isFile(appStatusFile))

    val eventLogFiles = listEventLogFiles(logDirPath)
    if (!skipVerifyEventLogFile(compressionCodecShortName, isCompleted)) {
      val allLines = mutable.ArrayBuffer[String]()
      eventLogFiles.foreach { file =>
        allLines.appendAll(readLinesFromEventLogFile(file.getPath, fileSystem))
      }

      assert(expectedLines === allLines)
    }
  }

  private def listEventLogFiles(logDirPath: Path): Seq[FileStatus] = {
    fileSystem.listStatus(logDirPath).filter(isEventLogFile)
      .sortBy(fs => getSequence(fs.getPath.getName))
  }
}
