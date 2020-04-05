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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SQLQuerySuiteWithSpeculation extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  override def sparkConf(): SparkConf = {
    super.sparkConf
      .set(config.SPECULATION_MULTIPLIER, 0.0)
      .set(config.SPECULATION_QUANTILE, 0.5)
      .set(config.SPECULATION_ENABLED, true)
  }

  test("SPARK-27194 SPARK-29302: Fix the issue that for dynamic partition overwrite, a " +
    "task would conflict with its speculative task") {
    withTable("ta", "tb") {
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
        SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
        sql("create table ta (c1 int, p1 int, p2 int) using parquet partitioned by (p1, p2)")
        val df = spark.sparkContext.parallelize(0 until 10000, 10)
          .map(index => (index, index % 10))
          .toDF("c1", "p2")
        df.write.mode("overwrite").saveAsTable("tb")
        sql("insert overwrite table ta partition(p1=1,p2) select * from tb")
        checkAnswer(sql("select c1, p2 from ta where p1='1'"), df)
      }
    }
  }
}
