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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.execution.command.{ShowTablesSuite => CommonShowTablesSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class ShowTablesSuite extends QueryTest with SharedSparkSession with CommonShowTablesSuite {
  override def catalog: String = "test_catalog_v2"
  override protected def namespaceColumn: String = "namespace"
  override protected def showSchema: StructType = {
    new StructType()
      .add("namespace", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryTableCatalog].getName)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE DATABASE $catalog.$namespace")
    sql(s"CREATE TABLE $catalog.$namespace.$table (name STRING, id INT) USING PARQUET")
  }

  protected override def afterAll(): Unit = {
    sql(s"DROP TABLE $catalog.$namespace.$table")
    sql(s"DROP DATABASE $catalog.$namespace")
    super.afterAll()
  }

  test("show table in a not existing namespace") {
    checkAnswer(sql(s"SHOW TABLES IN $catalog.bad_test"), Seq())
  }

  test("ShowTables: using v2 catalog") {
    withTable(s"$catalog.db.table_name") {
      spark.sql(s"CREATE TABLE $catalog.db.table_name (id bigint, data string) USING foo")
      runShowTablesSql(s"SHOW TABLES FROM $catalog.db", Seq(Row("db", "table_name")))
    }

    withTable(s"$catalog.n1.n2.db") {
      spark.sql(s"CREATE TABLE $catalog.n1.n2.db.table_name (id bigint, data string) USING foo")
      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.n1.n2.db",
        Seq(Row("n1.n2.db", "table_name")))
    }
  }

  test("ShowTables: using v2 catalog with a pattern") {
    withTable(
      s"$catalog.db.table",
      s"$catalog.db.table_name_1",
      s"$catalog.db.table_name_2",
      s"$catalog.db2.table_name_2") {
      spark.sql(s"CREATE TABLE $catalog.db.table (id bigint, data string) USING foo")
      spark.sql(s"CREATE TABLE $catalog.db.table_name_1 (id bigint, data string) USING foo")
      spark.sql(s"CREATE TABLE $catalog.db.table_name_2 (id bigint, data string) USING foo")
      spark.sql(s"CREATE TABLE $catalog.db2.table_name_2 (id bigint, data string) USING foo")

      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.db",
        Seq(
          Row("db", "table"),
          Row("db", "table_name_1"),
          Row("db", "table_name_2")))

      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.db LIKE '*name*'",
        Seq(Row("db", "table_name_1"), Row("db", "table_name_2")))

      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.db LIKE '*2'",
        Seq(Row("db", "table_name_2")))
    }
  }

  test("ShowTables: using v2 catalog, namespace doesn't exist") {
    runShowTablesSql(s"SHOW TABLES FROM $catalog.unknown", Seq())
  }
}
