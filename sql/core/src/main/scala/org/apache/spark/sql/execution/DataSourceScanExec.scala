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

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit._

import scala.collection.mutable.HashMap

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

trait DataSourceScanExec extends LeafExecNode {
  val relation: BaseRelation
  val tableIdentifier: Option[TableIdentifier]

  protected val nodeNamePrefix: String = ""

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Metadata that describes more details of this scan.
  protected def metadata: Map[String, String]

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), 100)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  private def redact(text: String): String = {
    Utils.redact(sqlContext.sessionState.conf.stringRedactionPattern, text)
  }

  /**
   * The data being read in.  This is to provide input to the tests in a way compatible with
   * [[InputRDDCodegen]] which all implementations used to extend.
   */
  def inputRDDs(): Seq[RDD[InternalRow]]
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    fullOutput: Seq[Attribute],
    requiredColumnsIndex: Seq[Int],
    filters: Set[Filter],
    handledFilters: Set[Filter],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with InputRDDCodegen {

  def output: Seq[Attribute] = requiredColumnsIndex.map(fullOutput)

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map( r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }

  // Input can be InternalRow, has to be turned into UnsafeRows.
  override protected val createUnsafeProjection: Boolean = true

  override def inputRDD: RDD[InternalRow] = rdd

  override val metadata: Map[String, String] = {
    val markedFilters = for (filter <- filters) yield {
      if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
    }
    Map(
      "ReadSchema" -> output.toStructType.catalogString,
      "PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
  }

  // Don't care about `rdd` and `tableIdentifier` when canonicalizing.
  override def doCanonicalize(): SparkPlan =
    copy(
      fullOutput.map(QueryPlan.normalizeExprId(_, fullOutput)),
      rdd = null,
      tableIdentifier = None)
}

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier identifier for the table in the metastore.
 */
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsColumnar: Boolean = {
    relation.fileFormat.supportBatch(relation.sparkSession, schema)
  }

  private lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  @transient private lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret = relation.location.listFiles(partitionFilters, dataFilters)
    if (relation.partitionSchemaOption.isDefined) {
      driverMetrics("numPartitions") = ret.length
    }
    driverMetrics("numFiles") = ret.map(_.files.size.toLong).sum
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  override lazy val outputPartitioning: Partitioning = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec match {
      case Some(spec) =>
        // For bucketed columns, `HashPartitioning` would be used only when ALL the bucketing
        // columns are being read from the table
        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          HashPartitioning(bucketColumns, spec.numBuckets)
        } else {
          UnknownPartitioning(0)
        }
      case _ =>
        UnknownPartitioning(0)
    }
  }

  @transient
  private val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)

    val withSelectedBucketsCount = relation.bucketSpec.map { spec =>
      val numSelectedBuckets = optionalBucketSet.map { b =>
        b.cardinality()
      } getOrElse {
        spec.numBuckets
      }
      metadata + ("SelectedBucketsCount" ->
        s"$numSelectedBuckets out of ${spec.numBuckets}")
    } getOrElse {
      metadata
    }

    withSelectedBucketsCount
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  ) ++ {
    if (relation.partitionSchemaOption.isDefined) {
      Some("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"))
    } else {
      None
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    if (needsUnsafeRowConversion) {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(index)
        iter.map( r => {
          numOutputRows += 1
          proj(r)
        })
      }
    } else {
      inputRDD.map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { batch =>
      numOutputRows += batch.numRows()
      batch
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  override def doCanonicalize(): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExprId(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(partitionFilters, output),
      optionalBucketSet,
      QueryPlan.normalizePredicates(dataFilters, output),
      None)
  }
}
