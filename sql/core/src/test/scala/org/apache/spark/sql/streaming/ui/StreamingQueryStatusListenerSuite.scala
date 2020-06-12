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

package org.apache.spark.sql.streaming.ui

import java.util.UUID

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark.sql.execution.ui.StreamingQueryStatusStore
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress, StreamTest}
import org.apache.spark.sql.streaming
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class StreamingQueryStatusListenerSuite extends StreamTest {

  test("onQueryStarted, onQueryProgress, onQueryTerminated") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

    // handle query started event
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val startEvent = new StreamingQueryListener.QueryStartedEvent(
      id, runId, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent)

    // result checking
    assert(queryStore.activeQueryUIData().length == 1)
    assert(queryStore.activeQueryUIData().exists(uiData =>
      uiData.summary.runId == runId && uiData.summary.name.equals("test")))

    // handle query progress event
    val progress = mock(classOf[StreamingQueryProgress], RETURNS_SMART_NULLS)
    when(progress.id).thenReturn(id)
    when(progress.runId).thenReturn(runId)
    when(progress.timestamp).thenReturn("2001-10-01T01:00:00.100Z")
    when(progress.inputRowsPerSecond).thenReturn(10.0)
    when(progress.processedRowsPerSecond).thenReturn(12.0)
    when(progress.batchId).thenReturn(2)
    when(progress.prettyJson).thenReturn("""{"a":1}""")
    val processEvent = new streaming.StreamingQueryListener.QueryProgressEvent(progress)
    listener.onQueryProgress(processEvent)

    // result checking
    val activeQuery = queryStore.activeQueryUIData().find(_.summary.runId == runId)
    assert(activeQuery.isDefined)
    assert(activeQuery.get.summary.isActive)
    assert(activeQuery.get.recentProgress.length == 1)
    assert(activeQuery.get.lastProgress.id == id)
    assert(activeQuery.get.lastProgress.runId == runId)
    assert(activeQuery.get.lastProgress.timestamp == "2001-10-01T01:00:00.100Z")
    assert(activeQuery.get.lastProgress.inputRowsPerSecond == 10.0)
    assert(activeQuery.get.lastProgress.processedRowsPerSecond == 12.0)
    assert(activeQuery.get.lastProgress.batchId == 2)
    assert(activeQuery.get.lastProgress.prettyJson == """{"a":1}""")

    // handle terminate event
    val terminateEvent = new StreamingQueryListener.QueryTerminatedEvent(id, runId, None)
    listener.onQueryTerminated(terminateEvent)

    assert(!queryStore.inactiveQueryUIData().head.summary.isActive)
    assert(queryStore.inactiveQueryUIData().head.summary.runId == runId)
    assert(queryStore.inactiveQueryUIData().head.summary.id == id)
  }

  test("same query start multiple times") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

    // handle first time start
    val id = UUID.randomUUID()
    val runId0 = UUID.randomUUID()
    val startEvent0 = new StreamingQueryListener.QueryStartedEvent(
      id, runId0, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent0)

    // handle terminate event
    val terminateEvent0 = new StreamingQueryListener.QueryTerminatedEvent(id, runId0, None)
    listener.onQueryTerminated(terminateEvent0)

    // handle second time start
    val runId1 = UUID.randomUUID()
    val startEvent1 = new StreamingQueryListener.QueryStartedEvent(
      id, runId1, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent1)

    // result checking
    assert(queryStore.activeQueryUIData().length == 1)
    assert(queryStore.inactiveQueryUIData().length == 1)
    assert(queryStore.activeQueryUIData().exists(_.summary.runId == runId1))
    assert(queryStore.activeQueryUIData().exists(uiData =>
      uiData.summary.runId == runId1 && uiData.summary.id == id))
    assert(queryStore.inactiveQueryUIData().head.summary.runId == runId0)
    assert(queryStore.inactiveQueryUIData().head.summary.id == id)
  }
}
