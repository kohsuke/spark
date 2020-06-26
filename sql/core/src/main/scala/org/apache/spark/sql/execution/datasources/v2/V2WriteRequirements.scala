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

import org.apache.spark.sql.{catalyst, AnalysisException}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NullOrdering, SortDirection, SortValue}
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering
import org.apache.spark.sql.internal.SQLConf

// this rule does resolution in the optimizer because some nodes like OverwriteByExpression
// must undergo the expression optimization before we can construct a logical write
object V2WriteRequirements extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case V2BatchWriteCommand(write: RequiresDistributionAndOrdering, query, false) =>
      val sqlConf = SQLConf.get
      val resolver = sqlConf.resolver

      val distribution = write.requiredDistribution match {
        case d: OrderedDistribution =>
          d.ordering.map(e => toCatalyst(e, query, resolver))
        case d: ClusteredDistribution =>
          d.clustering.map(e => toCatalyst(e, query, resolver))
        case _: UnspecifiedDistribution =>
          Array.empty[catalyst.expressions.Expression]
      }

      val queryWithDistribution = if (distribution.nonEmpty) {
        val numShufflePartitions = sqlConf.numShufflePartitions
        // repartition by expression will correctly pick range or hash partitioning
        // based on whether we have an ordered or clustered distribution
        RepartitionByExpression(distribution, query, numShufflePartitions)
      } else {
        query
      }

      val ordering = write.requiredOrdering.toSeq
        .map(e => toCatalyst(e, query, resolver))
        .asInstanceOf[Seq[catalyst.expressions.SortOrder]]

      val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
        Sort(ordering, global = false, queryWithDistribution)
      } else {
        queryWithDistribution
      }

      // set aligned flag to true to make the rule idempotent
      V2BatchWriteCommand(write, queryWithDistributionAndOrdering, aligned = true)
  }

  private def toCatalyst(
      expr: Expression,
      query: LogicalPlan,
      resolver: Resolver): catalyst.expressions.Expression = {
    expr match {
      case SortValue(child, direction, nullOrdering) =>
        val catalystChild = toCatalyst(child, query, resolver)
        SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Set.empty)
      case ref: FieldReference =>
        // this part is controversial as we perform resolution in the optimizer
        // we cannot perform this step in the analyzer since we need to optimize expressions
        // in nodes like OverwriteByExpression before constructing a logical write
        query.resolve(ref.parts, resolver) match {
          case Some(attr) => attr
          case None => throw new AnalysisException(s"Cannot resolve '$ref' using ${query.output}")
        }
      case _ =>
        throw new RuntimeException(s"$expr is not currently supported")
    }
  }

  private def toCatalyst(direction: SortDirection): catalyst.expressions.SortDirection = {
    direction match {
      case SortDirection.ASCENDING => catalyst.expressions.Ascending
      case SortDirection.DESCENDING => catalyst.expressions.Descending
    }
  }

  private def toCatalyst(nullOrdering: NullOrdering): catalyst.expressions.NullOrdering = {
    nullOrdering match {
      case NullOrdering.NULLS_FIRST => catalyst.expressions.NullsFirst
      case NullOrdering.NULLS_LAST => catalyst.expressions.NullsLast
    }
  }
}
