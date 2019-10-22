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

package org.apache.spark.sql.execution.ui

private[ui] object ToolTips {
  val SQL_TAB_DURATION =
    "Difference between start time and close time"

  val SQL_TAB_SUBMITTED =
    "Job submission time"

  val SQL_TAB_DESCRIPTION =
    "Description of submitted Job"

  val SQL_TAB_ID =
    "Unique sequential number of submitted Job"

  val SQL_TAB_RUNNING_JOB_IDS =
    "Id's of running jobs"

  val SQL_TAB_SUCCEEDED_JOB_IDS =
    "Id's of succeeded jobs"

  val SQL_TAB_FAILED_JOB_IDS =
    "Id's of failed jobs"
}
