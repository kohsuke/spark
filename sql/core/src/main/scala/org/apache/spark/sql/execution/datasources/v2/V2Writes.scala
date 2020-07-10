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

package org.apache.spark.sql.execution.datasources.v2

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.{LogicalWriteInfoImpl, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}

/**
 * A rule that constructs [[Write]]s.
 */
object V2Writes extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case AppendData(relation: DataSourceV2Relation, query, options, _) =>
      val writeBuilder = newWriteBuilder(relation.table, query, options)
      val write = writeBuilder.build()
      V2BatchWriteCommand(write, query)

    case OverwriteByExpression(relation: DataSourceV2Relation, deleteExpr, query, options, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).map {
        filter => DataSourceStrategy.translateFilter(deleteExpr,
          supportNestedPredicatePushdown = true).getOrElse(
          throw new AnalysisException(s"Cannot translate expression to source filter: $filter"))
      }.toArray

      val table = relation.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(filters) =>
          builder.truncate().build()
        case builder: SupportsOverwrite =>
          builder.overwrite(filters).build()
        case _ =>
          throw new SparkException(s"Table does not support overwrite by expression: $table")
      }

      V2BatchWriteCommand(write, query)

    case OverwritePartitionsDynamic(relation: DataSourceV2Relation, query, options, _) =>
      val table = relation.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
      }
      V2BatchWriteCommand(write, query)
  }

  private def newWriteBuilder(
      table: Table,
      query: LogicalPlan,
      writeOptions: Map[String, String]): WriteBuilder = {

    val info = LogicalWriteInfoImpl(
      queryId = UUID.randomUUID().toString,
      query.schema,
      writeOptions.asOptions)
    table.asWritable.newWriteBuilder(info)
  }

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }
}

// aligned flag is needed to keep idempotence of V2WriteRequirements
case class V2BatchWriteCommand(
    write: Write,
    query: LogicalPlan,
    aligned: Boolean = false) extends Command {

  override def children: Seq[LogicalPlan] = Seq(query)
}
