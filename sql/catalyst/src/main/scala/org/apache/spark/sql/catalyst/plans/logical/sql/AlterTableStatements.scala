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

package org.apache.spark.sql.catalyst.plans.logical.sql

import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, LogicalPlan}
import org.apache.spark.sql.sources.v2.Table
import org.apache.spark.sql.types.DataType

/**
 * Column data as parsed by ALTER TABLE ... ADD COLUMNS.
 */
case class QualifiedColType(name: Seq[String], dataType: DataType, comment: Option[String])

trait AlterTableStatement extends StatementRequiringCatalogAndTable {
  protected def tableChanges: Seq[TableChange]

  override def withCatalogAndTable(
      catalog: TableCatalog,
      ident: Identifier,
      table: Table): LogicalPlan = {
    AlterTable(catalog, ident, table, tableChanges)
  }
}

/**
 * ALTER TABLE ... ADD COLUMNS command, as parsed from SQL.
 */
case class AlterTableAddColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    columnsToAdd.map { col =>
      TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
    }
  }
}

/**
 * ALTER TABLE ... CHANGE COLUMN command, as parsed from SQL.
 */
case class AlterTableAlterColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    dataType: Option[DataType],
    comment: Option[String]) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    val typeChange = dataType.map { newDataType =>
      TableChange.updateColumnType(column.toArray, newDataType, true)
    }

    val commentChange = comment.map { newComment =>
      TableChange.updateColumnComment(column.toArray, newComment)
    }

    typeChange.toSeq ++ commentChange
  }
}

/**
 * ALTER TABLE ... RENAME COLUMN command, as parsed from SQL.
 */
case class AlterTableRenameColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    newName: String) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    Seq(TableChange.renameColumn(column.toArray, newName))
  }
}

/**
 * ALTER TABLE ... DROP COLUMNS command, as parsed from SQL.
 */
case class AlterTableDropColumnsStatement(
    tableName: Seq[String],
    columnsToDrop: Seq[Seq[String]]) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    columnsToDrop.map(col => TableChange.deleteColumn(col.toArray))
  }
}

/**
 * ALTER TABLE ... SET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableSetPropertiesStatement(
    tableName: Seq[String],
    properties: Map[String, String]) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    properties.map { case (key, value) =>
      TableChange.setProperty(key, value)
    }.toSeq
  }
}

/**
 * ALTER TABLE ... UNSET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableUnsetPropertiesStatement(
    tableName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    propertyKeys.map(key => TableChange.removeProperty(key))
  }
}

/**
 * ALTER TABLE ... SET LOCATION command, as parsed from SQL.
 */
case class AlterTableSetLocationStatement(
    tableName: Seq[String],
    location: String) extends AlterTableStatement {

  override def tableChanges: Seq[TableChange] = {
    Seq(TableChange.setProperty("location", location))
  }
}
