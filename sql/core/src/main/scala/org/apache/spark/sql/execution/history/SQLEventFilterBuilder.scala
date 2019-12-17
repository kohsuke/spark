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

package org.apache.spark.sql.execution.history

import scala.collection.mutable

import org.apache.spark.deploy.history.{EventFilter, EventFilterBuilder, JobEventFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * This class tracks live SQL executions, and pass the list to the [[SQLLiveEntitiesEventFilter]]
 * to help SQLLiveEntitiesEventFilter to reject finished SQL executions as well as relevant
 * jobs (+ stages/tasks/RDDs). Unlike BasicEventFilterBuilder, it doesn't concern about the status
 * of individual job - it only concerns whether SQL execution is finished or not.
 */
private[spark] class SQLEventFilterBuilder extends SparkListener with EventFilterBuilder {
  private val _liveExecutionToJobs = new mutable.HashMap[Long, mutable.Set[Int]]
  private val _jobToStages = new mutable.HashMap[Int, Seq[Int]]
  private val _stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  private val _stageToRDDs = new mutable.HashMap[Int, Seq[Int]]
  private val stages = new mutable.HashSet[Int]

  def liveExecutionToJobs: Map[Long, Set[Int]] = _liveExecutionToJobs.mapValues(_.toSet).toMap
  def jobToStages: Map[Int, Seq[Int]] = _jobToStages.toMap
  def stageToTasks: Map[Int, Set[Long]] = _stageToTasks.mapValues(_.toSet).toMap
  def stageToRDDs: Map[Int, Seq[Int]] = _stageToRDDs.toMap

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId

    val jobsForExecution = _liveExecutionToJobs.getOrElseUpdate(executionId,
      mutable.HashSet[Int]())
    jobsForExecution += jobId

    _jobToStages += jobStart.jobId -> jobStart.stageIds
    stages ++= jobStart.stageIds
    jobStart.stageIds.foreach { stageId => _stageToTasks += stageId -> mutable.HashSet[Long]() }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    if (stages.contains(stageId)) {
      val rddInfos = stageSubmitted.stageInfo.rddInfos
      _stageToRDDs += stageId -> rddInfos.map(_.id)
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (_stageToTasks.contains(taskStart.stageId)) {
      val curTasks = _stageToTasks(taskStart.stageId)
      curTasks += taskStart.taskInfo.taskId
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ => // Ignore
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    _liveExecutionToJobs += event.executionId -> mutable.HashSet[Int]()
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    _liveExecutionToJobs.remove(event.executionId).foreach { jobs =>
      val stagesToDrop = _jobToStages.filter(kv => jobs.contains(kv._1)).values.flatten
      _jobToStages --= jobs
      stages --= stagesToDrop
      _stageToTasks --= stagesToDrop
      _stageToRDDs --= stagesToDrop
    }
  }

  override def createFilter(): EventFilter = {
    SQLLiveEntitiesEventFilter(this)
  }
}

/**
 * This class rejects events which are related to the finished SQL executions based on the
 * given information.
 *
 * Note that acceptFn will not match the event instead of returning false if the event is related
 * to job but not coupled with live SQL executions, because the instance has the information about
 * jobs for live SQL executions which should be accepted, but don't know whether the job is
 * related to the finished SQL executions, or job is NOT related to the SQL executions. For this
 * case, it just gives up the decision and let other filters decide it.
 *
 * The events which are not related to the SQL execution will be considered as "Don't mind".
 */
private[spark] class SQLLiveEntitiesEventFilter(
    liveExecutionToJobs: Map[Long, Set[Int]],
    _jobToStages: Map[Int, Seq[Int]],
    _stageToTasks: Map[Int, Set[Long]],
    _stageToRDDs: Map[Int, Seq[Int]])
  extends JobEventFilter(None, _jobToStages, _stageToTasks, _stageToRDDs) with Logging {

  logDebug(s"live executions : ${liveExecutionToJobs.keySet}")
  logDebug(s"jobs in live executions : ${liveExecutionToJobs.values.flatten}")

  private val _acceptFn: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerSQLExecutionStart =>
      liveExecutionToJobs.contains(e.executionId)
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      liveExecutionToJobs.contains(e.executionId)
    case e: SparkListenerSQLExecutionEnd =>
      liveExecutionToJobs.contains(e.executionId)
    case e: SparkListenerDriverAccumUpdates =>
      liveExecutionToJobs.contains(e.executionId)

    case e if acceptFnForJobEvents.lift(e).contains(true) =>
      // NOTE: if acceptFnForJobEvents(e) returns false, we should leave it to "unmatched"
      true

    // these events are for finished batches so safer to ignore
    case _: StreamingQueryListener.QueryProgressEvent => false
  }

  override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = _acceptFn
}

private[spark] object SQLLiveEntitiesEventFilter {
  def apply(builder: SQLEventFilterBuilder): SQLLiveEntitiesEventFilter = {
    new SQLLiveEntitiesEventFilter(
      builder.liveExecutionToJobs,
      builder.jobToStages,
      builder.stageToTasks,
      builder.stageToRDDs)
  }
}
