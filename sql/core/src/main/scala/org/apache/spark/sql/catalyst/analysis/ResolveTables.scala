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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, DeleteFromTable, DescribeTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableAlterColumnStatement, AlterTableDropColumnsStatement, AlterTableRenameColumnStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, DeleteFromStatement, DescribeTableStatement, QualifiedColType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, TableChange}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField}

/**
 * Resolves tables from the multi-part identifiers in DDL/DML commands.
 *
 * For each SQL statement, this rule has 2 different code paths for v1 and v2 tables.
 */
class ResolveTables(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case AlterTableAddColumnsStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), cols) =>
      cols.foreach(c => assertTopLeveColumn(c.name, "AlterTableAddColumnsCommand"))
      AlterTableAddColumnsCommand(tblName.toV1Identifier, cols.map(convertToStructField))

    case AlterTableAddColumnsStatement(
         CatalogAndTable(catalog, tblName, Right(table)), cols) =>
      val changes = cols.map { col =>
        TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
      }
      AlterTable(catalog, tblName.toIdentifier, table, changes)

    // The v1 `AlterTableAddColumnsCommand` will check temp view and provide better error message.
    // Here we convert the statement to the v1 command to get the better error message.
    // TODO: apply the temp view check for all ALTER TABLE statements.
    case AlterTableAddColumnsStatement(tblName, cols) =>
      cols.foreach(c => assertTopLeveColumn(c.name, "AlterTableAddColumnsCommand"))
      AlterTableAddColumnsCommand(tblName.toV1Identifier, cols.map(convertToStructField))

    // TODO: we should fallback to the v1 `AlterTableChangeColumnCommand`.
    case AlterTableAlterColumnStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), colName, dataType, comment) =>
      throw new AnalysisException("ALTER COLUMN is only supported with v2 tables.")

    case AlterTableAlterColumnStatement(
         CatalogAndTable(catalog, tblName, Right(table)), colName, dataType, comment) =>
      val typeChange = dataType.map { newDataType =>
        TableChange.updateColumnType(colName.toArray, newDataType, true)
      }
      val commentChange = comment.map { newComment =>
        TableChange.updateColumnComment(colName.toArray, newComment)
      }
      AlterTable(catalog, tblName.toIdentifier, table, typeChange.toSeq ++ commentChange)

    case AlterTableRenameColumnStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), col, newName) =>
      throw new AnalysisException("RENAME COLUMN is only supported with v2 tables.")

    case AlterTableRenameColumnStatement(
         CatalogAndTable(catalog, tblName, Right(table)), col, newName) =>
      val changes = Seq(TableChange.renameColumn(col.toArray, newName))
      AlterTable(catalog, tblName.toIdentifier, table, changes)

    case AlterTableDropColumnsStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), cols) =>
      throw new AnalysisException("DROP COLUMN is only supported with v2 tables.")

    case AlterTableDropColumnsStatement(
         CatalogAndTable(catalog, tblName, Right(table)), cols) =>
      val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
      AlterTable(catalog, tblName.toIdentifier, table, changes)

    case AlterTableSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), props) =>
      AlterTableSetPropertiesCommand(tblName.toV1Identifier, props, isView = false)

    case AlterTableSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Right(table)), props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }
      AlterTable(catalog.asTableCatalog, tblName.toIdentifier, table, changes.toSeq)

    case AlterTableUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tblName.toV1Identifier, keys, ifExists, isView = false)

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Right(table)), keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      AlterTable(catalog, tblName.toIdentifier, table, changes)

    case AlterTableSetLocationStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), newLoc) =>
      AlterTableSetLocationCommand(tblName.toV1Identifier, None, newLoc)

    case AlterTableSetLocationStatement(
         CatalogAndTable(catalog, tblName, Right(table)), newLoc) =>
      val changes = Seq(TableChange.setProperty("location", newLoc))
      AlterTable(catalog.asTableCatalog, tblName.toIdentifier, table, changes)

    // The v1 `AlterTableSetLocationCommand` throws NuSuchTable exception at runtime, while the v2
    // command throws AnalysisException at analysis time. Here we convert to the v1 command to keep
    // the exception type unchanged.
    // TODO: unify the table not found exception for all ALTER TABLE commands.
    case AlterTableSetLocationStatement(tblName, newLoc) =>
      AlterTableSetLocationCommand(tblName.toV1Identifier, None, newLoc)

    case AlterViewSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Right(table)), props) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewSetPropertiesStatement(tblName, props) =>
      AlterTableSetPropertiesCommand(tblName.toV1Identifier, props, isView = true)

    case AlterViewUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, Right(table)), keys, ifExists) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewUnsetPropertiesStatement(tblName, keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tblName.toV1Identifier, keys, ifExists, isView = true)

    case DeleteFromStatement(
         CatalogAndTable(catalog, tblName, Left(v1Table)), tableAlias, condition) =>
      throw new AnalysisException("DELETE FROM is only supported with v2 tables.")

    case DeleteFromStatement(
         CatalogAndTable(catalog, tblName, Right(table)), tableAlias, condition) =>
      val relation = DataSourceV2Relation.create(table)
      val aliased = tableAlias.map(SubqueryAlias(_, relation)).getOrElse(relation)
      DeleteFromTable(aliased, condition)

    case DescribeTableStatement(
         CatalogAndTable(catalog, tblName, Right(table)), partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESC TABLE does not support partition for v2 tables.")
      }
      DescribeTable(table, isExtended)

    // If we can't convert `DescribeTableStatement` to v2 `DescribeTable`, blindly convert it to
    // v1 `DescribeTableCommand` because it can describe view/temp view as well.
    case DescribeTableStatement(tblName, partitionSpec, isExtended) =>
      DescribeTableCommand(tblName.toV1Identifier, partitionSpec, isExtended)
  }

  private def assertTopLeveColumn(colName: Seq[String], command: String): Unit = {
    if (colName.length > 1) {
      throw new AnalysisException(s"$command does not support nested column: ${colName.quoted}")
    }
  }

  private def convertToStructField(col: QualifiedColType): StructField = {
    val builder = new MetadataBuilder
    col.comment.foreach(builder.putString("comment", _))

    val cleanedDataType = HiveStringType.replaceCharType(col.dataType)
    if (col.dataType != cleanedDataType) {
      builder.putString(HIVE_TYPE_STRING, col.dataType.catalogString)
    }

    StructField(
      col.name.head,
      cleanedDataType,
      nullable = true,
      builder.build())
  }
}
