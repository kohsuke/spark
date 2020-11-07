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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

trait ShowTablesSuite extends QueryTest with SharedSparkSession {
  protected def catalog: String
  protected def namespaceColumn: String = "database"
  protected def namespace: String = "test"
  protected def tableColumn: String = "tableName"
  protected def table: String = "people"

  case class ShowRow(namespace: String, table: String, isTemporary: Boolean)

  protected def showSchema: StructType
  protected def getRows(showRows: Seq[ShowRow]): Seq[Row]

  protected def runShowTablesSql(sqlText: String, expected: Seq[ShowRow]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === showSchema)
    assert(df.collect() === getRows(expected))
  }

  test("show an existing table") {
    runShowTablesSql(s"SHOW TABLES IN $catalog.test", Seq(ShowRow(namespace, table, false)))
  }
}
