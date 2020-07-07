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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RemoveRedundantProjectsSuite extends QueryTest with SharedSparkSession {

  private def assertProjectExecCount(df: DataFrame, expected: Integer): Unit = {
    withClue(df.queryExecution) {
      val plan = df.queryExecution.executedPlan
      val actual = plan.collectWithSubqueries { case p: ProjectExec => p }.size
      assert(actual == expected)
    }
  }

  private def assertProjectExec(query: String, enabled: Integer, disabled: Integer): Unit = {
    val df = sql(query)
    assertProjectExecCount(df, enabled)
    val result1 = df.collect()
    withSQLConf(SQLConf.REMOVE_REDUNDANT_PROJECTS_ENABLED.key -> "false") {
      val df2 = sql(query)
      assertProjectExecCount(df2, disabled)
      QueryTest.sameRows(result1.toSeq, df2.collect().toSeq)
    }
  }

  private def withTestView(f: => Unit): Unit = {
    withTempPath { p =>
      val path = p.getAbsolutePath
      spark.range(100).selectExpr("id % 10 as key", "id * 2 as a",
        "id * 3 as b", "cast(id as string) as c").write.partitionBy("key").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("testView")
      f
    }
  }

  test("project") {
    withTestView {
      val query = "select * from testView"
      assertProjectExec(query, 0, 0)
    }
  }

  test("project with filter") {
    withTestView {
      val query = "select * from testView where a > 5"
      assertProjectExec(query, 0, 1)
    }
  }

  test("project with specific column ordering") {
    withTestView {
      val query = "select key, a, b, c from testView"
      assertProjectExec(query, 1, 1)
    }
  }

  test("project with extra columns") {
    withTestView {
      val query = "select a, b, c, key, a from testView"
      assertProjectExec(query, 1, 1)
    }
  }

  test("project with fewer columns") {
    withTestView {
      val query = "select a from testView where a > 3"
      assertProjectExec(query, 1, 1)
    }
  }

  test("aggregate without ordering requirement") {
    withTestView {
      val query = "select sum(a) as sum_a, key, last(b) as last_b " +
        "from (select key, a, b from testView where a > 100) group by key"
      assertProjectExec(query, 0, 1)
    }
  }

  test("aggregate with ordering requirement") {
    withTestView {
      val query = "select a, sum(b) as sum_b from testView group by a"
      assertProjectExec(query, 1, 1)
    }
  }

  test("join without ordering requirement") {
    withTestView {
      val query = "select t1.key, t2.key, t1.a, t2.b from (select key, a, b, c from testView)" +
        " as t1 join (select key, a, b, c from testView) as t2 on t1.c > t2.c and t1.key > 10"
      assertProjectExec(query, 1, 3)
    }
  }

  test("join with ordering requirement") {
    withTestView {
      val query = "select * from (select key, a, c, b from testView) as t1 join " +
        "(select key, a, b, c from testView) as t2 on t1.key = t2.key where t2.a > 50"
      assertProjectExec(query, 2, 2)
    }
  }

  test("window function") {
    withTestView {
      val query = "select key, avg(a) over (partition by key order by a " +
        "rows between 1 preceding and 1 following) as avg, b from testView"
      assertProjectExec(query, 1, 2)
    }
  }

  test("subquery") {
    withTestView {
      val query = "select * from testView where a in (select b from testView where key > 5)"
      assertProjectExec(query, 1, 1)
    }
  }
}
