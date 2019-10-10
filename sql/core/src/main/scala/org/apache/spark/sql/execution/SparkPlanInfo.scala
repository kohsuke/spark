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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, LocalShuffleReaderExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.internal.SQLConf

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
 */
@DeveloperApi
class SparkPlanInfo(
    val nodeName: String,
    val simpleString: String,
    val children: Seq[SparkPlanInfo],
    val metadata: Map[String, String],
    val metrics: Seq[SQLMetricInfo],
    val relation: Seq[SparkPlanInfo] = Seq()) {

  override def hashCode(): Int = {
    // hashCode of simpleString should be good enough to distinguish the plans from each other
    // within a plan
    simpleString.hashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: SparkPlanInfo =>
      nodeName == o.nodeName && simpleString == o.simpleString && children == o.children
    case _ => false
  }
}

private[execution] object SparkPlanInfo {

  def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case ReusedSubqueryExec(child) => child :: Nil
      case a: AdaptiveSparkPlanExec => a.executedPlan :: Nil
      case stage: QueryStageExec => stage.plan :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    val nodeName = plan match {
      case physicalOperator: WholeStageCodegenExec =>
        s"${plan.nodeName} (${physicalOperator.codegenStageId})"
      case _ => plan.nodeName
    }

    // dump the file scan metadata (e.g file path) to event log
    val metadata = plan match {
      case fileScan: FileSourceScanExec => fileScan.metadata
      case _ => Map[String, String]()
    }

    val relation = plan match {
      case inMemTab: InMemoryTableScanExec => Seq(fromSparkPlan(inMemTab.relation.cachedPlan))
      case _ => Seq()
    }

    new SparkPlanInfo(
      nodeName,
      plan.simpleString(SQLConf.get.maxToStringFields),
      children.map(fromSparkPlan),
      metadata,
      metrics)
      metadata, metrics, relation)
  }
}
