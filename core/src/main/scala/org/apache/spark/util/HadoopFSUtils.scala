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

package org.apache.spark.util

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics


private[spark] object HadoopFSUtils extends Logging {
  /**
   * Lists a collection of paths recursively. Picks the listing strategy adaptively depending
   * on the number of paths to list.
   *
   * This may only be called on the driver.
   *
   * @return for each input path, the set of discovered files for the path
   */

  def parallelListLeafFiles(sc: SparkContext, paths: Seq[Path], hadoopConf: Configuration,
    filter: PathFilter, areSQLRootPaths: Boolean, ignoreMissingFiles: Boolean,
    ignoreLocality: Boolean, maxParallelism: Int,
    filterFun: Option[String => Boolean] = None): Seq[(Path, Seq[FileStatus])] = {
    HiveCatalogMetrics.incrementParallelListingJobCount(1)

    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val serializedPaths = paths.map(_.toString)

    // Set the number of parallelism to prevent following file listing from generating many tasks
    // in case of large #defaultParallelism.
    val numParallelism = Math.min(paths.size, maxParallelism)

    val previousJobDescription = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val statusMap = try {
      val description = paths.size match {
        case 0 =>
          s"Listing leaf files and directories 0 paths"
        case 1 =>
          s"Listing leaf files and directories for 1 path:<br/>${paths(0)}"
        case s =>
          s"Listing leaf files and directories for $s paths:<br/>${paths(0)}, ..."
      }
      sc.setJobDescription(description) // TODO(holden): should we use jobgroup?
      sc
        .parallelize(serializedPaths, numParallelism)
        .mapPartitions { pathStrings =>
          val hadoopConf = serializableConfiguration.value
          pathStrings.map(new Path(_)).toSeq.map { path =>
            val leafFiles = listLeafFiles(
              contextOpt = None, // Can't execute parallel scans on workers
              path = path,
              hadoopConf = hadoopConf,
              filter = filter,
              ignoreMissingFiles = ignoreMissingFiles,
              ignoreLocality = ignoreLocality,
              isSQLRootPath = areSQLRootPaths,
              filterFun = filterFun,
              parallelismThreshold = Int.MaxValue,
              maxParallelism = 0)
            (path, leafFiles)
          }.iterator
        }.collect() // TODO(holden): should we use local itr here?
    } finally {
      sc.setJobDescription(previousJobDescription)
    }

    statusMap.toSeq
  }
  /**
   * Lists a single filesystem path recursively. If a Sparkcontext object is specified, this
   * function may launch Spark jobs to parallelize listing based on parallelismThreshold.
   *
   * If sessionOpt is None, this may be called on executors.
   *
   * @return all children of path that match the specified filter.
   */
  // scalastyle:off argcount
  def listLeafFiles(
      path: Path,
      hadoopConf: Configuration,
      filter: PathFilter,
      contextOpt: Option[SparkContext],
      ignoreMissingFiles: Boolean,
      ignoreLocality: Boolean,
      isSQLRootPath: Boolean,
      filterFun: Option[String => Boolean],
      parallelismThreshold: Int,
      maxParallelism: Int): Seq[FileStatus] = {
    // scalastyle:on argcount

    logTrace(s"Listing $path")
    val fs = path.getFileSystem(hadoopConf)

    // Note that statuses only include FileStatus for the files and dirs directly under path,
    // and does not include anything else recursively.
    val statuses: Array[FileStatus] = try {
      fs match {
        // DistributedFileSystem overrides listLocatedStatus to make 1 single call to namenode
        // to retrieve the file status with the file block location. The reason to still fallback
        // to listStatus is because the default implementation would potentially throw a
        // FileNotFoundException which is better handled by doing the lookups manually below.
        case (_: DistributedFileSystem | _: ViewFileSystem) if !ignoreLocality =>
          val remoteIter = fs.listLocatedStatus(path)
          new Iterator[LocatedFileStatus]() {
            def next(): LocatedFileStatus = remoteIter.next
            def hasNext(): Boolean = remoteIter.hasNext
          }.toArray
        case _ if !ignoreLocality =>
          // Try and use the accelerated code path even if it isn't known
          // to support it, and fall back.
          try {
            val remoteIter = fs.listLocatedStatus(path)
            new Iterator[LocatedFileStatus]() {
              def next(): LocatedFileStatus = remoteIter.next
              def hasNext(): Boolean = remoteIter.hasNext
            }.toArray
          } catch {
            case e: FileNotFoundException =>
              fs.listStatus(path)
          }
        case _ =>
          fs.listStatus(path)
      }
    } catch {
      // If we are listing a root path for SQL (e.g. a top level directory of a table), we need to
      // ignore FileNotFoundExceptions during this root level of the listing because
      //
      //  (a) certain code paths might construct an InMemoryFileIndex with root paths that
      //      might not exist (i.e. not all callers are guaranteed to have checked
      //      path existence prior to constructing InMemoryFileIndex) and,
      //  (b) we need to ignore deleted root paths during REFRESH TABLE, otherwise we break
      //      existing behavior and break the ability drop SessionCatalog tables when tables'
      //      root directories have been deleted (which breaks a number of Spark's own tests).
      //
      // If we are NOT listing a root path then a FileNotFoundException here means that the
      // directory was present in a previous level of file listing but is absent in this
      // listing, likely indicating a race condition (e.g. concurrent table overwrite or S3
      // list inconsistency).
      //
      // The trade-off in supporting existing behaviors / use-cases is that we won't be
      // able to detect race conditions involving root paths being deleted during
      // InMemoryFileIndex construction. However, it's still a net improvement to detect and
      // fail-fast on the non-root cases. For more info see the SPARK-27676 review discussion.
      case _: FileNotFoundException if isSQLRootPath || ignoreMissingFiles =>
        logWarning(s"The directory $path was not found. Was it deleted very recently?")
        Array.empty[FileStatus]
    }

    val filteredStatuses = filterFun match {
      case Some(shouldFilterOut) =>
        statuses.filterNot(status => shouldFilterOut(status.getPath.getName))
      case None =>
        statuses
    }

    val allLeafStatuses = {
      val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
      val nestedFiles: Seq[FileStatus] = contextOpt match {
        case Some(context) if dirs.size > parallelismThreshold =>
          parallelListLeafFiles(
            context,
            dirs.map(_.getPath),
            hadoopConf = hadoopConf,
            filter = filter,
            areSQLRootPaths = false,
            ignoreMissingFiles = ignoreMissingFiles,
            ignoreLocality = ignoreLocality,
            filterFun = filterFun,
            maxParallelism = maxParallelism
          ).flatMap(_._2)
        case _ =>
          dirs.flatMap { dir =>
            listLeafFiles(
              path = dir.getPath,
              hadoopConf = hadoopConf,
              filter = filter,
              contextOpt = contextOpt,
              ignoreMissingFiles = ignoreMissingFiles,
              ignoreLocality = ignoreLocality,
              isSQLRootPath = false,
              filterFun = filterFun,
              parallelismThreshold = parallelismThreshold,
              maxParallelism = maxParallelism)
          }
      }
      val allFiles = topLevelFiles ++ nestedFiles
      if (filter != null) allFiles.filter(f => filter.accept(f.getPath)) else allFiles
    }

    val missingFiles = mutable.ArrayBuffer.empty[String]
    val filteredLeafStatuses = filterFun match {
      case Some(shouldFilterOut) =>
        allLeafStatuses.filterNot(
          status => shouldFilterOut(status.getPath.getName))
      case None =>
        allLeafStatuses
    }
    val resolvedLeafStatuses = filteredLeafStatuses.flatMap {
      case f: LocatedFileStatus =>
        Some(f)

      // NOTE:
      //
      // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
      //   operations, calling `getFileBlockLocations` does no harm here since these file system
      //   implementations don't actually issue RPC for this method.
      //
      // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
      //   be a big deal since we always use to `parallelListLeafFiles` when the number of
      //   paths exceeds threshold.
      case f if !ignoreLocality =>
        // The other constructor of LocatedFileStatus will call FileStatus.getPermission(),
        // which is very slow on some file system (RawLocalFileSystem, which is launch a
        // subprocess and parse the stdout).
        try {
          val locations = fs.getFileBlockLocations(f, 0, f.getLen).map { loc =>
            // Store BlockLocation objects to consume less memory
            if (loc.getClass == classOf[BlockLocation]) {
              loc
            } else {
              new BlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
            }
          }
          val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
            f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
          if (f.isSymlink) {
            lfs.setSymlink(f.getSymlink)
          }
          Some(lfs)
        } catch {
          case _: FileNotFoundException if ignoreMissingFiles =>
            missingFiles += f.getPath.toString
            None
        }

      case f => Some(f)
    }

    if (missingFiles.nonEmpty) {
      logWarning(
        s"the following files were missing during file scan:\n  ${missingFiles.mkString("\n  ")}")
    }

    resolvedLeafStatuses.toSeq
  }
}
