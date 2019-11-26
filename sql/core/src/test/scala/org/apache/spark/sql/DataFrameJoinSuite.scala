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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameJoinSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("join - join using") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")

    checkAnswer(
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - join using multiple columns") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")

    checkAnswer(
      df.join(df2, Seq("int", "int2")),
      Row(1, 2, "1", "2") :: Row(2, 3, "2", "3") :: Row(3, 4, "3", "4") :: Nil)
  }

  test("join - sorted columns not in join's outputSet") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str_sort").as('df1)
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as('df2)
    val df3 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as('df3)

    checkAnswer(
      df.join(df2, $"df1.int" === $"df2.int", "outer").select($"df1.int", $"df2.int2")
        .orderBy('str_sort.asc, 'str.asc),
      Row(null, 6) :: Row(1, 3) :: Row(3, null) :: Nil)

    checkAnswer(
      df2.join(df3, $"df2.int" === $"df3.int", "inner")
        .select($"df2.int", $"df3.int").orderBy($"df2.str".desc),
      Row(5, 5) :: Row(1, 1) :: Nil)
  }

  test("join - join using multiple columns and specifying join type") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str")

    checkAnswer(
      df.join(df2, Seq("int", "str"), "inner"),
      Row(1, "1", 2, 3) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left"),
      Row(1, "1", 2, 3) :: Row(3, "3", 4, null) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "right"),
      Row(1, "1", 2, 3) :: Row(5, "5", null, 6) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "outer"),
      Row(1, "1", 2, 3) :: Row(3, "3", 4, null) :: Row(5, "5", null, 6) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left_semi"),
      Row(1, "1", 2) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "semi"),
      Row(1, "1", 2) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "left_anti"),
      Row(3, "3", 4) :: Nil)

    checkAnswer(
      df.join(df2, Seq("int", "str"), "anti"),
      Row(3, "3", 4) :: Nil)
  }

  test("join - cross join") {
    val df1 = Seq((1, "1"), (3, "3")).toDF("int", "str")
    val df2 = Seq((2, "2"), (4, "4")).toDF("int", "str")

    checkAnswer(
      df1.crossJoin(df2),
      Row(1, "1", 2, "2") :: Row(1, "1", 4, "4") ::
        Row(3, "3", 2, "2") :: Row(3, "3", 4, "4") :: Nil)

    checkAnswer(
      df2.crossJoin(df1),
      Row(2, "2", 1, "1") :: Row(2, "2", 3, "3") ::
        Row(4, "4", 1, "1") :: Row(4, "4", 3, "3") :: Nil)
  }

  test("broadcast join hint using broadcast function") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")

    // equijoin - should be converted into broadcast join
    val plan1 = df1.join(broadcast(df2), "key").queryExecution.sparkPlan
    assert(plan1.collect { case p: BroadcastHashJoinExec => p }.size === 1)

    // no join key -- should not be a broadcast join
    val plan2 = df1.crossJoin(broadcast(df2)).queryExecution.sparkPlan
    assert(plan2.collect { case p: BroadcastHashJoinExec => p }.size === 0)

    // planner should not crash without a join
    broadcast(df1).queryExecution.sparkPlan

    // SPARK-12275: no physical plan for BroadcastHint in some condition
    withTempPath { path =>
      df1.write.parquet(path.getCanonicalPath)
      val pf1 = spark.read.parquet(path.getCanonicalPath)
      assert(df1.crossJoin(broadcast(pf1)).count() === 4)
    }
  }

  test("broadcast join hint using Dataset.hint") {
    // make sure a giant join is not broadcastable
    val plan1 =
      spark.range(10e10.toLong)
        .join(spark.range(10e10.toLong), "id")
        .queryExecution.executedPlan
    assert(plan1.collect { case p: BroadcastHashJoinExec => p }.size == 0)

    // now with a hint it should be broadcasted
    val plan2 =
      spark.range(10e10.toLong)
        .join(spark.range(10e10.toLong).hint("broadcast"), "id")
        .queryExecution.executedPlan
    assert(plan2.collect { case p: BroadcastHashJoinExec => p }.size == 1)
  }

  test("join - outer join conversion") {
    val df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str").as("a")
    val df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("b")

    // outer -> left
    val outerJoin2Left = df.join(df2, $"a.int" === $"b.int", "outer").where($"a.int" >= 3)
    assert(outerJoin2Left.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, LeftOuter, _, _) => j }.size === 1)
    checkAnswer(
      outerJoin2Left,
      Row(3, 4, "3", null, null, null) :: Nil)

    // outer -> right
    val outerJoin2Right = df.join(df2, $"a.int" === $"b.int", "outer").where($"b.int" >= 3)
    assert(outerJoin2Right.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, RightOuter, _, _) => j }.size === 1)
    checkAnswer(
      outerJoin2Right,
      Row(null, null, null, 5, 6, "5") :: Nil)

    // outer -> inner
    val outerJoin2Inner = df.join(df2, $"a.int" === $"b.int", "outer").
      where($"a.int" === 1 && $"b.int2" === 3)
    assert(outerJoin2Inner.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, Inner, _, _) => j }.size === 1)
    checkAnswer(
      outerJoin2Inner,
      Row(1, 2, "1", 1, 3, "1") :: Nil)

    // right -> inner
    val rightJoin2Inner = df.join(df2, $"a.int" === $"b.int", "right").where($"a.int" > 0)
    assert(rightJoin2Inner.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, Inner, _, _) => j }.size === 1)
    checkAnswer(
      rightJoin2Inner,
      Row(1, 2, "1", 1, 3, "1") :: Nil)

    // left -> inner
    val leftJoin2Inner = df.join(df2, $"a.int" === $"b.int", "left").where($"b.int2" > 0)
    assert(leftJoin2Inner.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, Inner, _, _) => j }.size === 1)
    checkAnswer(
      leftJoin2Inner,
      Row(1, 2, "1", 1, 3, "1") :: Nil)
  }

  test("process outer join results using the non-nullable columns in the join input") {
    // Filter data using a non-nullable column from a right table
    val df1 = Seq((0, 0), (1, 0), (2, 0), (3, 0), (4, 0)).toDF("id", "count")
    val df2 = Seq(Tuple1(0), Tuple1(1)).toDF("id").groupBy("id").count
    checkAnswer(
      df1.join(df2, df1("id") === df2("id"), "left_outer").filter(df2("count").isNull),
      Row(2, 0, null, null) ::
      Row(3, 0, null, null) ::
      Row(4, 0, null, null) :: Nil
    )

    // Coalesce data using non-nullable columns in input tables
    val df3 = Seq((1, 1)).toDF("a", "b")
    val df4 = Seq((2, 2)).toDF("a", "b")
    checkAnswer(
      df3.join(df4, df3("a") === df4("a"), "outer")
        .select(coalesce(df3("a"), df3("b")), coalesce(df4("a"), df4("b"))),
      Row(1, null) :: Row(null, 2) :: Nil
    )
  }

  test("SPARK-16991: Full outer join followed by inner join produces wrong results") {
    val a = Seq((1, 2), (2, 3)).toDF("a", "b")
    val b = Seq((2, 5), (3, 4)).toDF("a", "c")
    val c = Seq((3, 1)).toDF("a", "d")
    val ab = a.join(b, Seq("a"), "fullouter")
    checkAnswer(ab.join(c, "a"), Row(3, null, 4, 1) :: Nil)
  }

  test("SPARK-17685: WholeStageCodegenExec throws IndexOutOfBoundsException") {
    val df = Seq((1, 1, "1"), (2, 2, "3")).toDF("int", "int2", "str")
    val df2 = Seq((1, 1, "1"), (2, 3, "5")).toDF("int", "int2", "str")
    val limit = 1310721
    val innerJoin = df.limit(limit).join(df2.limit(limit), Seq("int", "int2"), "inner")
      .agg(count($"int"))
    checkAnswer(innerJoin, Row(1) :: Nil)
  }

  test("SPARK-23087: don't throw Analysis Exception in CheckCartesianProduct when join condition " +
    "is false or null") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val df = spark.range(10)
      val dfNull = spark.range(10).select(lit(null).as("b"))
      df.join(dfNull, $"id" === $"b", "left").queryExecution.optimizedPlan

      val dfOne = df.select(lit(1).as("a"))
      val dfTwo = spark.range(10).select(lit(2).as("b"))
      dfOne.join(dfTwo, $"a" === $"b", "left").queryExecution.optimizedPlan
    }
  }

  test("SPARK-24385: Resolve ambiguity in self-joins with EqualNullSafe") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val df = spark.range(2)
      // this throws an exception before the fix
      df.join(df, df("id") <=> df("id")).queryExecution.optimizedPlan
    }
  }

  def extractLeftDeepInnerJoins(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case j @ Join(left, right, _: InnerLike, _, _) => right +: extractLeftDeepInnerJoins(left)
    case Filter(_, child) => extractLeftDeepInnerJoins(child)
    case Project(_, child) => extractLeftDeepInnerJoins(child)
    case _ => Seq(plan)
  }

  test("SPARK-24690 enables star schema detection even if CBO disabled") {
    withTable("r0", "r1", "r2", "r3") {
      withTempDir { dir =>

        withSQLConf(
            SQLConf.STARSCHEMA_DETECTION.key -> "true",
            SQLConf.CBO_ENABLED.key -> "false",
            SQLConf.PLAN_STATS_ENABLED.key -> "true") {

          val path = dir.getAbsolutePath

          // Collects column statistics first
          spark.range(300).selectExpr("id AS a", "id AS b", "id AS c")
            .write.mode("overwrite").parquet(s"$path/r0")
          spark.read.parquet(s"$path/r0").write.saveAsTable("r0")
          spark.sql("ANALYZE TABLE r0 COMPUTE STATISTICS FOR COLUMNS a, b, c")

          spark.range(10).selectExpr("id AS a", "id AS d")
            .write.mode("overwrite").parquet(s"$path/r1")
          spark.read.parquet(s"$path/r1").write.saveAsTable("r1")
          spark.sql("ANALYZE TABLE r1 COMPUTE STATISTICS FOR COLUMNS a")

          spark.range(50).selectExpr("id AS b", "id AS e")
            .write.mode("overwrite").parquet(s"$path/r2")
          spark.read.parquet(s"$path/r2").write.saveAsTable("r2")
          spark.sql("ANALYZE TABLE r2 COMPUTE STATISTICS FOR COLUMNS b")

          spark.range(1).selectExpr("id AS c", "id AS f")
            .write.mode("overwrite").parquet(s"$path/r3")
          spark.read.parquet(s"$path/r3").write.saveAsTable("r3")
          spark.sql("ANALYZE TABLE r3 COMPUTE STATISTICS FOR COLUMNS c")

          val resultDf = sql(
            s"""SELECT * FROM r0, r1, r2, r3
               |  WHERE
               |    r0.a = r1.a AND
               |    r1.d >= 3 AND
               |    r0.b = r2.b AND
               |    r2.e >= 5 AND
               |    r0.c = r3.c AND
               |    r3.f <= 100
             """.stripMargin)

          val optimized = resultDf.queryExecution.optimizedPlan
          val optJoins = extractLeftDeepInnerJoins(optimized)
          val joinOrder = optJoins
            .flatMap(_.collect { case p: LogicalRelation => p.catalogTable }.head)
            .map(_.identifier.identifier)

          assert(joinOrder === Seq("r2", "r1", "r3", "r0"))
        }
      }
    }
  }
}
