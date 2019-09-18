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
package org.apache.spark.sql.execution.datasources.v2.csv

import java.util

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType

class CSVDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CSVFileFormat]

  override def shortName(): String = "csv"

  override def loadTable(properties: util.Map[String, String]): Table = {
    val paths = getPaths(properties)
    val tableName = getTableName(paths)
    CSVTable(tableName, sparkSession, properties, paths, None, fallbackFileFormat)
  }

  override def loadTable(
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val paths = getPaths(properties)
    val tableName = getTableName(paths)
    CSVTable(tableName, sparkSession, properties, paths, Some(schema), fallbackFileFormat)
  }
}
