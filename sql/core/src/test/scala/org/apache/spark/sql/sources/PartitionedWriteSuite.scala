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

package org.apache.spark.sql.sources

import java.io.File
import java.sql.Timestamp

import scala.util.{Success, Try}

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.{DebugFilesystem, SparkConf, SparkContext, SparkEnv, TestUtils}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtilsBase}
import org.apache.spark.util.Utils

private class OnlyDetectCustomPathFileCommitProtocol(jobId: String, path: String)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path)
    with Serializable with Logging {

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new Exception("there should be no custom partition path")
  }
}

class PartitionedWriteSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("write many partitions") {
    val path = Utils.createTempDir()
    path.delete()

    val df = spark.range(100).select($"id", lit(1).as("data"))
    df.write.partitionBy("id").save(path.getCanonicalPath)

    checkAnswer(
      spark.read.load(path.getCanonicalPath),
      (0 to 99).map(Row(1, _)).toSeq)

    Utils.deleteRecursively(path)
  }

  test("write many partitions with repeats") {
    val path = Utils.createTempDir()
    path.delete()

    val base = spark.range(100)
    val df = base.union(base).select($"id", lit(1).as("data"))
    df.write.partitionBy("id").save(path.getCanonicalPath)

    checkAnswer(
      spark.read.load(path.getCanonicalPath),
      (0 to 99).map(Row(1, _)).toSeq ++ (0 to 99).map(Row(1, _)).toSeq)

    Utils.deleteRecursively(path)
  }

  test("partitioned columns should appear at the end of schema") {
    withTempPath { f =>
      val path = f.getAbsolutePath
      Seq(1 -> "a").toDF("i", "j").write.partitionBy("i").parquet(path)
      assert(spark.read.parquet(path).schema.map(_.name) == Seq("j", "i"))
    }
  }

  test("maxRecordsPerFile setting in non-partitioned write path") {
    withTempDir { f =>
      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", 1).mode("overwrite").parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", 2).mode("overwrite").parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 2)

      spark.range(start = 0, end = 4, step = 1, numPartitions = 1)
        .write.option("maxRecordsPerFile", -1).mode("overwrite").parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 1)
    }
  }

  test("maxRecordsPerFile setting in dynamic partition writes") {
    withTempDir { f =>
      spark.range(start = 0, end = 4, step = 1, numPartitions = 1).selectExpr("id", "id id1")
        .write
        .partitionBy("id")
        .option("maxRecordsPerFile", 1)
        .mode("overwrite")
        .parquet(f.getAbsolutePath)
      assert(TestUtils.recursiveList(f).count(_.getAbsolutePath.endsWith("parquet")) == 4)
    }
  }

  test("append data to an existing partitioned table without custom partition path") {
    withTable("t") {
      withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[OnlyDetectCustomPathFileCommitProtocol].getName) {
        Seq((1, 2)).toDF("a", "b").write.partitionBy("b").saveAsTable("t")
        // if custom partition path is detected by the task, it will throw an Exception
        // from OnlyDetectCustomPathFileCommitProtocol above.
        Seq((3, 2)).toDF("a", "b").write.mode("append").partitionBy("b").saveAsTable("t")
      }
    }
  }

  test("timeZone setting in dynamic partition writes") {
    def checkPartitionValues(file: File, expected: String): Unit = {
      val dir = file.getParentFile()
      val value = ExternalCatalogUtils.unescapePathName(
        dir.getName.substring(dir.getName.indexOf("=") + 1))
      assert(value == expected)
    }
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((1, ts)).toDF("i", "ts")
    withTempPath { f =>
      df.write.partitionBy("ts").parquet(f.getAbsolutePath)
      val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
      assert(files.length == 1)
      checkPartitionValues(files.head, "2016-12-01 00:00:00")
    }
    withTempPath { f =>
      df.write.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
        .partitionBy("ts").parquet(f.getAbsolutePath)
      val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
      assert(files.length == 1)
      // use timeZone option utcTz.getId to format partition value.
      checkPartitionValues(files.head, "2016-12-01 08:00:00")
    }
    withTempPath { f =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        df.write.partitionBy("ts").parquet(f.getAbsolutePath)
        val files = TestUtils.recursiveList(f).filter(_.getAbsolutePath.endsWith("parquet"))
        assert(files.length == 1)
        // if there isn't timeZone option, then use session local timezone.
        checkPartitionValues(files.head, "2016-12-01 08:00:00")
      }
    }
  }
}

private class DetectDynamicStagingTaskPartitionPathCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (dynamicPartitionOverwrite && isSpeculationEnabled) {
      val partitionPathSet = dynamicStagingTaskFiles
        .map(taskFile => getDynamicPartitionPath(taskFile, taskContext))
        .map(_.toUri.getPath.stripPrefix(stagingDir.toUri.getPath +
          Path.SEPARATOR))
      assert(partitionPathSet.equals(partitionPaths))
    }
    super.commitTask(taskContext)
  }
}

/**
 * This file system is used to simulate the scene that for dynamic partition overwrite operation
 * with speculation enabled, rename from dynamic staging files to final files failed for the task,
 * whose taskId and attemptId are 0, because another task has renamed a dynamic staging file to the
 * final file.
 */
class AnotherTaskRenamedForFirstTaskFirstAttemptFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    if (src.getName.startsWith("part-")) {
      Try {
        // File name format is part-$split%05d-$jobId$ext
        val taskId = src.getName.split("-").apply(1).toInt
        val attemptId = src.getParent.getName.split("-").last.toInt
        taskId == 0 && attemptId == 0
      } match {
        case Success(shouldRenameFailed) if shouldRenameFailed =>
          super.rename(src, dst)
          false
        case _ => super.rename(src, dst)
      }
    } else {
      super.rename(src, dst)
    }
  }
}

/**
 * This file system is used to simulate the scene that for dynamic partition overwrite operation
 * with speculation enabled, rename from dynamic staging files to final files failed for the task,
 * whose taskId and attemptId are 0, and there is no another task has renamed a dynamic staging
 * file to the final file.
 */
class RenameFailedForFirstTaskFirstAttemptFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    if (src.getName.startsWith("part-")) {
      Try {
        // File name format is part-$split%05d-$jobId$ext
        val taskId = src.getName.split("-").apply(1).toInt
        val attemptId = src.getParent.getName.split("-").last.toInt
        taskId == 0 && attemptId == 0
      } match {
        case Success(shouldRenameFailed) if shouldRenameFailed => false
        case _ => super.rename(src, dst)
      }
    } else {
      super.rename(src, dst)
    }
  }
}

class SpeculationEnabledDynamicPartitionedWriteSuite extends QueryTest with SQLTestUtilsBase {
  import testImplicits._

  override def spark: SparkSession = _spark
  protected var _spark: SparkSession = _

  /**
   * Referred the sparkConf method of SharedSparkSessionBase.
   */
  protected def sparkConf(): SparkConf = {
    val conf = new SparkConf()
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set(config.SPECULATION_MULTIPLIER, 0.0)
      .set(config.SPECULATION_QUANTILE, 0.5)
      .set(config.SPECULATION_ENABLED, true)
    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // set master to local[thread, maxRetries] to enable task retry.
    _spark = new SparkSession(new SparkContext("local[2, 2]", "test", sparkConf()))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    _spark.stop()
  }

  test("SPARK-27194 SPARK-29302: Fix the issue that for dynamic partition overwrite, a " +
    "task would conflict with its speculative task") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[DetectDynamicStagingTaskPartitionPathCommitProtocol].getName) {
      withTable("t") {
        val df = spark.sparkContext.parallelize(0 until 10000, 10)
          .map(index => (index, index % 10))
          .toDF("c1", "p1")
        df.write
          .partitionBy("p1")
          .mode("overwrite")
          .saveAsTable("t")
        checkAnswer(sql("select * from t"), df)
      }
    }
  }
}

class RenameFailedSpeculateDynamicPartitionedWriteSuite
  extends SpeculationEnabledDynamicPartitionedWriteSuite {
  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.hadoop.fs.file.impl",
        classOf[RenameFailedForFirstTaskFirstAttemptFileSystem].getName)
  }
}

class AnotherTaskRenamedSpeculateDynamicPartitionedWriteSuite
  extends SpeculationEnabledDynamicPartitionedWriteSuite {
  override protected def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.hadoop.fs.file.impl",
        classOf[AnotherTaskRenamedForFirstTaskFirstAttemptFileSystem].getName)
  }
}
