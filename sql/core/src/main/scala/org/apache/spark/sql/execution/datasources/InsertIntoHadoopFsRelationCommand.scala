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

package org.apache.spark.sql.execution.datasources

import java.io.IOException
import java.util.Date

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.InsertFileSourceConflictException
import org.apache.spark.internal.io.{FileCommitProtocol, FileSourceWriteDesc, HadoopMapReduceCommitProtocol}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.util.SchemaUtils

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitions partial partitioning spec for write. This defines the scope of partition
 *                         overwrites: when the spec is empty, all partitions are overwritten.
 *                         When it covers a prefix of the partition keys, only partitions matching
 *                         the prefix are overwritten.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex],
    outputColumnNames: Seq[String])
  extends DataWritingCommand {
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }

    val pathExists = fs.exists(qualifiedOutputPath)

    val parameters = CaseInsensitiveMap(options)

    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(sparkSession.sessionState.conf.partitionOverwriteMode)
    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    val dynamicPartitionOverwrite = enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      staticPartitions.size < partitionColumns.length

    val escapedStaticPartitionKVs = partitionColumns
      .filter(c => staticPartitions.contains(c.name))
      .map { attr =>
        val escapedKey = ExternalCatalogUtils.escapePathName(attr.name)
        val escapedValue = ExternalCatalogUtils.escapePathName(staticPartitions.get(attr.name).get)
        (escapedKey, escapedValue)
      }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputPath.toString,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite,
      fileSourceWriteDesc = Some(FileSourceWriteDesc(true, escapedStaticPartitionKVs)))

    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
          false
        } else {
          detectConflict(fs, outputPath, escapedStaticPartitionKVs)
          if (dynamicPartitionOverwrite) {
            // For dynamic partition overwrite, do not delete partition directories ahead.
            true
          } else {
            deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
            true
          }
        }
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        detectConflict(fs, outputPath, escapedStaticPartitionKVs)
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
        val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
        if (partitionsTrackedByCatalog) {
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }

      val updatedPartitionPaths =
        FileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            qualifiedOutputPath.toString, customPartitionLocations, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
          options = options)


      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Set(staticPathFragment))
      } else {
        refreshUpdatedPartitions(updatedPartitionPaths)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.catalog.refreshByPath(outputPath.toString)

      if (catalogTable.nonEmpty) {
        CommandUtils.updateTableStats(sparkSession, catalogTable.get)
      }

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name) match {
          case Some(value) =>
            Some(escapePathName(p.name) + "=" + escapePathName(value))
          case None =>
            None
        }
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }

  /**
   * Detect the conflict when there are several InsertIntoHadoopFsRelation operations
   * write to same partition in the same table or a non-partitioned table concurrently.
   */
  private def detectConflict(
      fs: FileSystem,
      path: Path,
      staticPartitionKVs: Seq[(String, String)]): Unit = {
    for (i <- 0 until partitionColumns.size) {
      Some(".spark-staging-" + i)
        .map(stagingPath => new Path(path, stagingPath))
        .foreach { stagingDir =>
          if (fs.exists(stagingDir)) {
            val subPath = HadoopMapReduceCommitProtocol.getEscapedStaticPartitionPath(
              staticPartitionKVs.slice(0, i))
            val checkedPath = if (!subPath.isEmpty) {
              new Path(stagingDir, subPath)
            } else {
              stagingDir
            }
            if (fs.exists(checkedPath)) {
              throwConflictException(fs, stagingDir, i, staticPartitionKVs)
            }
          }
        }
    }
  }

  private def throwConflictException(
      fs: FileSystem,
      stagingDir: Path,
      depth: Int,
      staticPartitionKVs: Seq[(String, String)]): Unit = {
    val conflictedPaths = ListBuffer[Path]()
    val currentPath = if (depth == staticPartitionKVs.size || staticPartitionKVs.size == 0) {
      stagingDir
    } else {
      new Path(stagingDir, HadoopMapReduceCommitProtocol.getEscapedStaticPartitionPath(
        staticPartitionKVs.slice(0, staticPartitionKVs.size - depth)))
    }

    findConflictedStagingOutputPaths(fs, currentPath, depth, conflictedPaths)

    val pathsInfo = conflictedPaths.toList
      .map { path =>
        val absolutePath = path.toUri.getRawPath
        val relativePath = absolutePath.substring(absolutePath.lastIndexOf(stagingDir.getName))
        var appId: Option[String] = None
        try {
          val files = fs.listStatus(path)
          if (files.size > 0) {
            appId = Some(files.apply(0).getPath.getName)
          }
        } catch {
          case e: Exception => logWarning("Exception occurred", e)
        }
        (relativePath, appId.getOrElse("Not Found"),
          new Date(fs.getFileStatus(path).getModificationTime))
      }

    throw new InsertFileSourceConflictException(
      s"""
         | Conflict is detected, some other conflicted output path(s) under tablePath:
         | ($outputPath) existed.
         | Relative path, appId and last modification time information is shown as below:
         | ${pathsInfo}.
         | There may be two possibilities:
         | 1. Another InsertDataSource operation is executing, you need wait for it to
         |    complete.
         | 2. This dir is belong to a killed application and not be cleaned up gracefully.
         |
         | Please check the last modification time and use given appId to judge whether
         | relative application is running now. If not, you should delete responding path
         | without recursive manually.
         |""".stripMargin)
  }

  /**
   * Find relative staging output paths, which is conflicted with current
   * InsertIntoHadoopFsRelation operation.
   */
  private def findConflictedStagingOutputPaths(
      fs: FileSystem,
      path: Path,
      depth: Int,
      paths: ListBuffer[Path]): Unit = {
    try {
      if (fs.exists(path)) {
        if (depth == 0) {
          paths += path
        } else {
          for (file <- fs.listStatus(path)) {
            findConflictedStagingOutputPaths(fs, file.getPath, depth - 1, paths)
          }
        }
      }
    } catch {
      case e: Exception =>
        logWarning("Exception occurred when finding conflicted staging output paths.", e)
    }
  }
}
