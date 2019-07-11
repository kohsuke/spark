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

package org.apache.spark.sql.sources.v2

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// this is currently in the spark-sql module because the read and write API is not in catalyst
// TODO(rdblue): when the v2 source API is in catalyst, merge with TestTableCatalog/InMemoryTable
class TestInMemoryTableCatalog extends TableCatalog {
  import CatalogV2Implicits._

  private val tables: util.Map[Identifier, InMemoryTable] =
    new ConcurrentHashMap[Identifier, InMemoryTable]()
  private var _name: Option[String] = None

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = Some(name)
  }

  override def name: String = _name.get

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tables.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    Option(tables.get(ident)) match {
      case Some(table) =>
        table
      case _ =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident)
    }

    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Catalog $name: Partitioned tables are not supported")
    }

    val table = new InMemoryTable(s"$name.${ident.quoted}", schema, properties)

    tables.put(ident, table)

    table
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    Option(tables.get(ident)) match {
      case Some(table) =>
        val properties = CatalogV2Util.applyPropertiesChanges(table.properties, changes)
        val schema = CatalogV2Util.applySchemaChanges(table.schema, changes)

        // fail if the last column in the schema was dropped
        if (schema.fields.isEmpty) {
          throw new IllegalArgumentException(s"Cannot drop all fields")
        }

        val newTable = new InMemoryTable(table.name, schema, properties, table.data)

        tables.put(ident, newTable)

        newTable
      case _ =>
        throw new NoSuchTableException(ident)
    }
  }

  override def dropTable(ident: Identifier): Boolean = Option(tables.remove(ident)).isDefined

  def clearTables(): Unit = {
    tables.clear()
  }
}
