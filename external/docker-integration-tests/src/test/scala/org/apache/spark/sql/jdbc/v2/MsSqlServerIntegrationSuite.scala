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

package org.apache.spark.sql.jdbc.v2

import java.sql.Connection

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., 2019-GA-ubuntu-16.04):
 * {{{
 *   MSSQLSERVER_DOCKER_IMAGE_NAME=2019-GA-ubuntu-16.04
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2*MsSqlServerIntegrationSuite"
 * }}}
 */
@DockerTest
class MsSqlServerIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSparkSession {

  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MSSQLSERVER_DOCKER_IMAGE_NAME",
      "mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04")
    override val env = Map(
      "SA_PASSWORD" -> "Sapass123",
      "ACCEPT_EULA" -> "Y"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1433

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:sqlserver://$ip:$port;user=sa;password=Sapass123;"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.mssql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.mssql.url", db.getJdbcUrl(dockerIp, externalPort))

  override val connectionTimeout = timeout(7.minutes)
  override def dataPreparation(conn: Connection): Unit = {}

  test("ALTER TABLE - update column nullability") {
    withTable("mssql.alt_table") {
      sql("CREATE TABLE mssql.alt_table (ID STRING NOT NULL) USING _")
      sql("ALTER TABLE mssql.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("mssql.alt_table")
      val expectedSchema = new StructType().add("ID", StringType, nullable = true)
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      val msg = intercept[AnalysisException] {
        sql("ALTER TABLE mssql.alt_table ALTER COLUMN bad_column DROP NOT NULL")
      }.getMessage
      assert(msg.contains("Cannot update missing field bad_column"))
    }
    // Update column nullability in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE mssql.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Table not found"))
  }

  test("ALTER TABLE - rename column") {
    withTable("mssql.alt_table") {
      sql("CREATE TABLE mssql.alt_table (ID STRING NOT NULL) USING _")
      sql("ALTER TABLE mssql.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("mssql.alt_table")
      val expectedSchema = new StructType().add("ID", StringType, nullable = true)
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      val msg = intercept[AnalysisException] {
        sql("ALTER TABLE mssql.alt_table ALTER COLUMN bad_column DROP NOT NULL")
      }.getMessage
      assert(msg.contains("Cannot update missing field bad_column"))
    }
    // Update column nullability in not existing table
    val msg = intercept[AnalysisException] {
      sql(s"ALTER TABLE mssql.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }.getMessage
    assert(msg.contains("Table not found"))
  }
}
