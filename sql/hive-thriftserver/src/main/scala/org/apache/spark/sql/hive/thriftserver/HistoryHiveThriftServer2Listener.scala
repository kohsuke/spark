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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.{ExecutionInfo, ExecutionState, SessionInfo}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

private[thriftserver] class HistoryHiveThriftServer2Listener(
                                           val conf: SparkConf) extends SparkListener {

  private val sessionList = new mutable.HashMap[String, SessionInfo]
  private val executionList = new mutable.HashMap[String, ExecutionInfo]
  private val retainedStatements = conf.get(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT)
  private val retainedSessions = conf.get(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT)

  def getOnlineSessionNum: Int = {
    sessionList.count(_._2.finishTimestamp == 0)
  }

  def isExecutionActive(execInfo: ExecutionInfo): Boolean = {
    !(execInfo.state == ExecutionState.FAILED ||
      execInfo.state == ExecutionState.CANCELED ||
      execInfo.state == ExecutionState.CLOSED)
  }

  /**
    * When an error or a cancellation occurs, we set the finishTimestamp of the statement.
    * Therefore, when we count the number of running statements, we need to exclude errors and
    * cancellations and count all statements that have not been closed so far.
    */
  def getTotalRunning: Int = {
  //  executionList.values().asScala.count(isExecutionActive(_))
    executionList.count {
      case (_, v) => isExecutionActive(v)
    }
  }

  def getSessionList: Seq[SessionInfo] = synchronized { sessionList.values.toSeq }

  def getSession(sessionId: String): Option[SessionInfo] = {
    Some(sessionList.get(sessionId))
  }

  def getExecutionList: Seq[ExecutionInfo] = synchronized { executionList.values.toSeq }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    for {
      props <- Option(jobStart.properties)
      groupId <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
      (_, info) <- executionList if info.groupId == groupId
    } {
      info.jobId += jobStart.jobId.toString
      info.groupId = groupId
    }
  }

  def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
    synchronized {
      val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
      sessionList.put(sessionId, info)
      trimSessionIfNecessary()
    }
  }

  def onSessionClosed(sessionId: String): Unit = synchronized {
    sessionList(sessionId).finishTimestamp = System.currentTimeMillis
    trimSessionIfNecessary()
  }

  def onStatementStart(
                        id: String,
                        sessionId: String,
                        statement: String,
                        groupId: String,
                        userName: String = "UNKNOWN"): Unit = synchronized {
    val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
    info.state = ExecutionState.STARTED
    executionList.put(id, info)
    trimExecutionIfNecessary()
    sessionList(sessionId).totalExecution += 1
    executionList(id).groupId = groupId
  }

  def onStatementParsed(id: String, executionPlan: String): Unit = synchronized {
    executionList(id).executePlan = executionPlan
    executionList(id).state = ExecutionState.COMPILED
  }

  def onStatementCanceled(id: String): Unit = synchronized {
    executionList(id).finishTimestamp = System.currentTimeMillis
    executionList(id).state = ExecutionState.CANCELED
    trimExecutionIfNecessary()
  }

  def onStatementError(id: String, errorMsg: String, errorTrace: String): Unit = synchronized {
    executionList(id).finishTimestamp = System.currentTimeMillis
    executionList(id).detail = errorMsg
    executionList(id).state = ExecutionState.FAILED
    trimExecutionIfNecessary()
  }

  def onStatementFinish(id: String): Unit = synchronized {
    executionList(id).finishTimestamp = System.currentTimeMillis
    executionList(id).state = ExecutionState.FINISHED
    trimExecutionIfNecessary()
  }

  def onOperationClosed(id: String): Unit = synchronized {
    executionList(id).closeTimestamp = System.currentTimeMillis
    executionList(id).state = ExecutionState.CLOSED
  }

  private def trimExecutionIfNecessary() = {
    if (executionList.size > retainedStatements) {
      val toRemove = math.max(retainedStatements / 10, 1)
      executionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        executionList.remove(s._1)
      }
    }
  }

  private def trimSessionIfNecessary() = {
    if (sessionList.size > retainedSessions) {
      val toRemove = math.max(retainedSessions / 10, 1)
      sessionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        sessionList.remove(s._1)
      }
    }

  }
}