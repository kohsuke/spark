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

package org.apache.spark.status.api.v1.sql

import java.util.Date
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import scala.util.{Failure, Success, Try}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SQLAppStatusStore, SQLExecutionUIData, SQLPlanMetric}
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlResource extends BaseAppResource {

  val WHOLE_STAGE_CODEGEN = "WholeStageCodegen"

  @GET
  def sqlList(
      @DefaultValue("false") @QueryParam("details") details: Boolean,
      @DefaultValue("true") @QueryParam("planDescription") planDescription: Boolean,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int): Seq[ExecutionData] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      sqlStore.executionsList(offset, length).map(prepareExecutionData(_,
        details = details, planDescription = planDescription))
    }
  }

  @GET
  @Path("{executionId:\\d+}")
  def sql(
      @PathParam("executionId") execId: Long,
      @DefaultValue("false") @QueryParam("details") details: Boolean,
      @DefaultValue("true") @QueryParam("planDescription")
      planDescription: Boolean): ExecutionData = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val graph = sqlStore.planGraph(execId)
      val nodeIdAndWSCGIdMap: Map[Long, Option[Long]] = getNodeIdAndWSCGIdMap(graph)
      sqlStore
        .execution(execId)
        .map(prepareExecutionData(_, nodeIdAndWSCGIdMap, details, planDescription))
        .getOrElse(throw new NotFoundException("unknown execution id: " + execId))
    }
  }

  private def printableMetrics(
      sqlPlanMetrics: Seq[SQLPlanMetric],
      metricValues: Map[Long, String],
      nodeIdAndWSCGIdMap: Map[Long, Option[Long]]): Seq[MetricDetails] = {

    def getMetric(metricValues: Map[Long, String], accumulatorId: Long,
                  metricName: String): Option[Metric] = {
      metricValues.get(accumulatorId).map( mv => {
        val metricValue = if (mv.startsWith("\n")) mv.substring(1, mv.length) else mv
        Metric(metricName, metricValue)
      })
    }

    val groupedMap: Map[(Long, String), Seq[SQLPlanMetric]] =
      sqlPlanMetrics.groupBy[(Long, String)](
        sqlPlanMetric => (sqlPlanMetric.nodeId.getOrElse(-1), sqlPlanMetric.nodeName.getOrElse("")))

    val metrics = groupedMap.mapValues[Seq[Metric]](sqlPlanMetrics =>
      sqlPlanMetrics.flatMap(m => getMetric(metricValues, m.accumulatorId, m.name.trim)))

    val metricDetails = metrics.map {
      case ((nodeId: Long, nodeName: String), metrics: Seq[Metric]) =>
        val wholeStageCodegenId = nodeIdAndWSCGIdMap.get(nodeId).flatten
        MetricDetails(nodeId = nodeId, nodeName = nodeName.trim, wholeStageCodegenId, metrics)
    }.toSeq

    metricDetails.sortBy(_.nodeId).reverse
  }

  private def prepareExecutionData(
    exec: SQLExecutionUIData,
    nodeIdAndWSCGIdMap: Map[Long, Option[Long]] = Map.empty,
    details: Boolean,
    planDescription: Boolean): ExecutionData = {

    var running = Seq[Int]()
    var completed = Seq[Int]()
    var failed = Seq[Int]()

    exec.jobs.foreach {
      case (id, JobExecutionStatus.RUNNING) =>
        running = running :+ id
      case (id, JobExecutionStatus.SUCCEEDED) =>
        completed = completed :+ id
      case (id, JobExecutionStatus.FAILED) =>
        failed = failed :+ id
      case _ =>
    }

    val status = if (exec.jobs.size == completed.size) {
      "COMPLETED"
    } else if (failed.nonEmpty) {
      "FAILED"
    } else {
      "RUNNING"
    }

    val duration = exec.completionTime.getOrElse(new Date()).getTime - exec.submissionTime
    val planDetails = if (details && planDescription) exec.physicalPlanDescription else ""
    val metrics =
      if (details) {
        printableMetrics(exec.metrics, exec.metricValues, nodeIdAndWSCGIdMap)
      } else {
        Seq.empty
      }

    new ExecutionData(
      exec.executionId,
      status,
      exec.description,
      planDetails,
      new Date(exec.submissionTime),
      duration,
      running,
      completed,
      failed,
      metrics)
  }

  private def getNodeIdAndWSCGIdMap(graph: SparkPlanGraph): Map[Long, Option[Long]] = {
    val wscgNodes = graph.allNodes.filter(_.name.trim.startsWith(WHOLE_STAGE_CODEGEN))
    val nodeIdAndWSCGIdMap: Map[Long, Option[Long]] = wscgNodes.flatMap {
      _ match {
        case x: SparkPlanGraphCluster => x.nodes.map(_.id -> getWholeStageCodegenId(x.name.trim))
        case _ => Seq.empty
      }
    }.toMap

    nodeIdAndWSCGIdMap
  }

  private def getWholeStageCodegenId(wscgNodeName: String): Option[Long] = {
    Try(wscgNodeName.substring(
      s"$WHOLE_STAGE_CODEGEN (".length, wscgNodeName.length - 1).toLong) match {
      case Success(wscgId) => Some(wscgId)
      case Failure(t) => None
    }
  }

}
