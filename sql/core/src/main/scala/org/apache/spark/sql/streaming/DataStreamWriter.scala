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

package org.apache.spark.sql.streaming

import java.util.Locale
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamWriter[T] private[sql](ds: Dataset[T]) {
  import DataStreamWriter._

  private val df = ds.toDF()

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   * <ul>
   * <li> `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be
   * written to the sink.</li>
   * <li> `OutputMode.Complete()`: all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time there are some updates.</li>
   * <li> `OutputMode.Update()`: only the rows that were updated in the streaming
   * DataFrame/Dataset will be written to the sink every time there are some updates.
   * If the query doesn't contain aggregations, it will be equivalent to
   * `OutputMode.Append()` mode.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = {
    this.outputMode = outputMode
    this
  }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   * <ul>
   * <li> `append`: only the new rows in the streaming DataFrame/Dataset will be written to
   * the sink.</li>
   * <li> `complete`: all the rows in the streaming DataFrame/Dataset will be written to the sink
   * every time there are some updates.</li>
   * <li> `update`: only the rows that were updated in the streaming DataFrame/Dataset will
   * be written to the sink every time there are some updates. If the query doesn't
   * contain aggregations, it will be equivalent to `append` mode.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: String): DataStreamWriter[T] = {
    this.outputMode = InternalOutputModes(outputMode)
    this
  }

  /**
   * Set the trigger for the stream query. The default value is `ProcessingTime(0)` and it will run
   * the query as fast as possible.
   *
   * Scala Example:
   * {{{
   *   df.writeStream.trigger(ProcessingTime("10 seconds"))
   *
   *   import scala.concurrent.duration._
   *   df.writeStream.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * Java Example:
   * {{{
   *   df.writeStream().trigger(ProcessingTime.create("10 seconds"))
   *
   *   import java.util.concurrent.TimeUnit
   *   df.writeStream().trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.0.0
   */
  def trigger(trigger: Trigger): DataStreamWriter[T] = {
    this.trigger = trigger
    this
  }

  /**
   * Specifies the name of the [[StreamingQuery]] that can be started with `start()`.
   * This name must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 2.0.0
   */
  def queryName(queryName: String): DataStreamWriter[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /**
   * Specifies the underlying output data source.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamWriter[T] = {
    this.source = source
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   *
   * <ul>
   * <li> year=2016/month=01/</li>
   * <li> year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to format timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamWriter[T] = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to format timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to format timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def start(path: String): StreamingQuery = {
    if (!df.sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw new AnalysisException("There is a 'path' option set and start() is called with a " +
        "path parameter. Either remove the path option, or call start() without the parameter. " +
        s"To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.")
    }
    startInternal(Some(path))
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream. Throws a `TimeoutException` if the following conditions are met:
   *  - Another run of the same streaming query, that is a streaming query
   *    sharing the same checkpoint location, is already active on the same
   *    Spark Driver
   *  - The SQL configuration `spark.sql.streaming.stopActiveRunOnRestart`
   *    is enabled
   *  - The active run cannot be stopped within the timeout controlled by
   *    the SQL configuration `spark.sql.streaming.stopTimeout`
   *
   * @since 2.0.0
   */
  @throws[TimeoutException]
  def start(): StreamingQuery = startInternal(None)

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * table as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 3.1.0
   */
  @throws[TimeoutException]
  def saveAsTable(tableName: String): StreamingQuery = {
    this.source = SOURCE_NAME_TABLE
    this.tableName = tableName
    startInternal(None)
  }

  private def startInternal(path: Option[String]): StreamingQuery = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "write files of Hive data source directly.")
    }

    if (source == SOURCE_NAME_TABLE) {
      assertNotPartitioned(SOURCE_NAME_TABLE)

      import df.sparkSession.sessionState.analyzer.CatalogAndIdentifier

      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      val originalMultipartIdentifier = df.sparkSession.sessionState.sqlParser
        .parseMultipartIdentifier(tableName)
      val CatalogAndIdentifier(catalog, identifier) = originalMultipartIdentifier

      // Currently we don't create a logical streaming writer node in logical plan, so cannot rely
      // on analyzer to resolve it. Directly lookup only for temp view to provide clearer message.
      // TODO (SPARK-27484): we should add the writing node before the plan is analyzed.
      if (df.sparkSession.sessionState.catalog.isTempView(originalMultipartIdentifier)) {
        throw new AnalysisException(s"Temporary view $tableName doesn't support streaming write")
      }

      val tableInstance = catalog.asTableCatalog.loadTable(identifier)

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      val sink = tableInstance match {
        case t: SupportsWrite if t.supports(STREAMING_WRITE) => t
        case t => throw new AnalysisException(s"Table $tableName doesn't support streaming " +
          s"write - $t")
      }

      startQuery(sink, extraOptions)
    } else if (source == SOURCE_NAME_MEMORY) {
      assertNotPartitioned(SOURCE_NAME_MEMORY)
      if (extraOptions.get("queryName").isEmpty) {
        throw new AnalysisException("queryName must be specified for memory sink")
      }
      val sink = new MemorySink()
      val resultDf = Dataset.ofRows(df.sparkSession, new MemoryPlan(sink, df.schema.toAttributes))
      val recoverFromChkpoint = outputMode == OutputMode.Complete()
      val query = startQuery(sink, extraOptions, recoverFromCheckpoint = recoverFromChkpoint)
      resultDf.createOrReplaceTempView(query.name)
      query
    } else if (source == SOURCE_NAME_FOREACH) {
      assertNotPartitioned(SOURCE_NAME_FOREACH)
      val sink = ForeachWriterTable[T](foreachWriter, ds.exprEnc)
      startQuery(sink, extraOptions)
    } else if (source == SOURCE_NAME_FOREACH_BATCH) {
      assertNotPartitioned(SOURCE_NAME_FOREACH_BATCH)
      if (trigger.isInstanceOf[ContinuousTrigger]) {
        throw new AnalysisException(s"'$source' is not supported with continuous trigger")
      }
      val sink = new ForeachBatchSink[T](foreachBatchWriter, ds.exprEnc)
      startQuery(sink, extraOptions)
    } else {
      val cls = DataSource.lookupDataSource(source, df.sparkSession.sessionState.conf)
      val disabledSources =
        Utils.stringToSeq(df.sparkSession.sqlContext.conf.disabledV2StreamingWriters)
      val useV1Source = disabledSources.contains(cls.getCanonicalName) ||
        // file source v2 does not support streaming yet.
        classOf[FileDataSourceV2].isAssignableFrom(cls)

      val optionsWithPath = if (path.isEmpty) {
        extraOptions
      } else {
        extraOptions + ("path" -> path.get)
      }

      val sink = if (classOf[TableProvider].isAssignableFrom(cls) && !useV1Source) {
        val provider = cls.getConstructor().newInstance().asInstanceOf[TableProvider]
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          source = provider, conf = df.sparkSession.sessionState.conf)
        val finalOptions = sessionOptions.filterKeys(!optionsWithPath.contains(_)).toMap ++
          optionsWithPath.originalMap
        val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
        val outputSchema = if (provider.supportsExternalMetadata()) {
          Some(df.schema)
        } else {
          None
        }
        val table = DataSourceV2Utils.getTableFromProvider(
          provider, dsOptions, userSpecifiedSchema = outputSchema)
        import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
        table match {
          case table: SupportsWrite if table.supports(STREAMING_WRITE) =>
            table
          case _ => createV1Sink(optionsWithPath)
        }
      } else {
        createV1Sink(optionsWithPath)
      }

      startQuery(sink, optionsWithPath)
    }
  }

  private def startQuery(
      sink: Table,
      newOptions: CaseInsensitiveMap[String],
      recoverFromCheckpoint: Boolean = true): StreamingQuery = {
    val useTempCheckpointLocation = SOURCES_ALLOW_ONE_TIME_QUERY.contains(source)

    df.sparkSession.sessionState.streamingQueryManager.startQuery(
      newOptions.get("queryName"),
      newOptions.get("checkpointLocation"),
      df,
      newOptions.originalMap,
      sink,
      outputMode,
      useTempCheckpointLocation = useTempCheckpointLocation,
      recoverFromCheckpointLocation = recoverFromCheckpoint,
      trigger = trigger)
  }

  private def createV1Sink(optionsWithPath: CaseInsensitiveMap[String]): Sink = {
    val ds = DataSource(
      df.sparkSession,
      className = source,
      options = optionsWithPath.originalMap,
      partitionColumns = normalizedParCols.getOrElse(Nil))
    ds.createSink(outputMode)
  }

  /**
   * Sets the output of the streaming query to be processed using the provided writer object.
   * object. See [[org.apache.spark.sql.ForeachWriter]] for more details on the lifecycle and
   * semantics.
   * @since 2.0.0
   */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] = {
    this.source = SOURCE_NAME_FOREACH
    this.foreachWriter = if (writer != null) {
      ds.sparkSession.sparkContext.clean(writer)
    } else {
      throw new IllegalArgumentException("foreach writer cannot be null")
    }
    this
  }

  /**
   * :: Experimental ::
   *
   * (Scala-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only the in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier.
   * The batchId can be used deduplicate and transactionally write the output
   * (that is, the provided Dataset) to external systems. The output Dataset is guaranteed
   * to exactly same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 2.4.0
   */
  @Evolving
  def foreachBatch(function: (Dataset[T], Long) => Unit): DataStreamWriter[T] = {
    this.source = SOURCE_NAME_FOREACH_BATCH
    if (function == null) throw new IllegalArgumentException("foreachBatch function cannot be null")
    this.foreachBatchWriter = function
    this
  }

  /**
   * :: Experimental ::
   *
   * (Java-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only the in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier.
   * The batchId can be used deduplicate and transactionally write the output
   * (that is, the provided Dataset) to external systems. The output Dataset is guaranteed
   * to exactly same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 2.4.0
   */
  @Evolving
  def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): DataStreamWriter[T] = {
    foreachBatch((batchDs: Dataset[T], batchId: Long) => function.call(batchDs, batchId))
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { cols =>
    cols.map(normalize(_, "Partition"))
  }

  /**
   * The given column name may not be equal to any of the existing column names if we were in
   * case-insensitive context. Normalize the given column name to the real one so that we don't
   * need to care about case sensitivity afterwards.
   */
  private def normalize(columnName: String, columnType: String): String = {
    val validColumnNames = df.logicalPlan.output.map(_.name)
    validColumnNames.find(df.sparkSession.sessionState.analyzer.resolver(_, columnName))
      .getOrElse(throw new AnalysisException(s"$columnType column $columnName not found in " +
        s"existing columns (${validColumnNames.mkString(", ")})"))
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw new AnalysisException(s"'$operation' does not support partitioning")
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName

  private var tableName: String = null

  private var outputMode: OutputMode = OutputMode.Append

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var foreachWriter: ForeachWriter[T] = null

  private var foreachBatchWriter: (Dataset[T], Long) => Unit = null

  private var partitioningColumns: Option[Seq[String]] = None
}

object DataStreamWriter {
  val SOURCE_NAME_MEMORY = "memory"
  val SOURCE_NAME_FOREACH = "foreach"
  val SOURCE_NAME_FOREACH_BATCH = "foreachBatch"
  val SOURCE_NAME_CONSOLE = "console"
  val SOURCE_NAME_TABLE = "table"
  val SOURCE_NAME_NOOP = "noop"

  // these writer sources are also used for one-time query, hence allow temp checkpoint location
  val SOURCES_ALLOW_ONE_TIME_QUERY = Seq(SOURCE_NAME_MEMORY, SOURCE_NAME_FOREACH,
    SOURCE_NAME_FOREACH_BATCH, SOURCE_NAME_CONSOLE, SOURCE_NAME_NOOP)
}
