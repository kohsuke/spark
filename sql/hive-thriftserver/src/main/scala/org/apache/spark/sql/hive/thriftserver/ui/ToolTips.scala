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

package org.apache.spark.sql.hive.thriftserver.ui

private[ui] object ToolTips {
  val THRIFT_SERVER_FINISH_TIME =
    "Execution finish time, before fetching the results"

  val THRIFT_SERVER_CLOSE_TIME =
    "Operation close time after fetching the results"

  val THRIFT_SERVER_EXECUTION =
    "Difference between start time and finish time"

  val THRIFT_SERVER_DURATION =
    "Difference between start time and close time"

  val THRIFT_SESSION_TOTAL_EXECUTE =
    "Number of operations submitted in this session."

  val THRIFT_SESSION_START_TIME =
    "Session Start time, on launching the session."

  val THRIFT_SESSION_FINISH_TIME =
    "Time that the session was closed."

  val THRIFT_SESSION_DURATION =
    "Total duration the session has been active, from the time session" +
      " was started till it was closed."

}
