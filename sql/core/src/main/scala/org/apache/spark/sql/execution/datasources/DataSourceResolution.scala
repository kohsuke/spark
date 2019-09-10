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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2.{CatalogManager, LookupCatalog}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{CastSupport, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, ShowTables, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, CreateTableAsSelectStatement, CreateTableStatement, DeleteFromStatement, DescribeColumnStatement, DescribeTableStatement, DropTableStatement, DropViewStatement, QualifiedColType, ReplaceTableAsSelectStatement, ReplaceTableStatement, ShowNamespacesStatement, ShowTablesStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, DescribeColumnCommand, DescribeTableCommand, DropTableCommand, ShowDatabasesCommand, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField, StructType}

case class DataSourceResolution(
    conf: SQLConf,
    catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with CastSupport with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists) =>
      provider match {
        case V1Provider(_) =>
          // the source is v1, the identifier has no catalog, and there is no default v2 catalog
          val tableDesc = buildCatalogTable(table, schema, partitionCols, bucketSpec, properties,
            provider, options, location, comment, ifNotExists)
          val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

          CreateTable(tableDesc, mode, None)

        case _ =>
          c.withCatalog(sessionCatalog, c.tableName)
      }

    case c @ CreateTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists) =>
      provider match {
        case V1Provider(_) =>
          // the source is v1, the identifier has no catalog, and there is no default v2 catalog
          val tableDesc = buildCatalogTable(table, new StructType, partitionCols, bucketSpec,
            properties, provider, options, location, comment, ifNotExists)
          val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

          CreateTable(tableDesc, mode, Some(query))

        case _ =>
          c.withCatalog(sessionCatalog, c.tableName)
      }

    case DescribeColumnStatement(
        AsTableIdentifier(tableName), colName, isExtended) =>
      DescribeColumnCommand(tableName, colName, isExtended)

    case DescribeColumnStatement(
        CatalogObjectIdentifier(Some(catalog), ident), colName, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case DescribeTableStatement(AsTableIdentifier(tableName), partitionSpec, isExtended) =>
      DescribeTableCommand(tableName, partitionSpec, isExtended)

    case r @ ReplaceTableStatement(AsTableIdentifier(table), _, _, _, _, _, _, _, _, _) =>
      r.provider match {
        case V1Provider(_) =>
          throw new AnalysisException(
            s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
              s" API. Write provider name: ${r.provider}, identifier: $table.")

        case _ =>
          r.withCatalog(sessionCatalog, r.tableName)
      }

    case r @ ReplaceTableAsSelectStatement(AsTableIdentifier(table), _, _, _, _, _, _, _, _, _) =>
      r.provider match {
        case V1Provider(_) =>
          throw new AnalysisException(
            s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
              s" API. Write provider name: ${r.provider}, identifier: $table.")

        case _ =>
          r.withCatalog(sessionCatalog, r.tableName)
      }

    case DropTableStatement(AsTableIdentifier(tableName), ifExists, purge) =>
      DropTableCommand(tableName, ifExists, isView = false, purge)

    case DropViewStatement(CatalogObjectIdentifier(Some(catalog), ident), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view $ident " +
          s"because view support in catalog has not been implemented yet")

    case DropViewStatement(AsTableIdentifier(tableName), ifExists) =>
      DropTableCommand(tableName, ifExists, isView = true, purge = false)

    case AlterTableSetPropertiesStatement(AsTableIdentifier(table), properties) =>
      AlterTableSetPropertiesCommand(table, properties, isView = false)

    case AlterViewSetPropertiesStatement(AsTableIdentifier(table), properties) =>
      AlterTableSetPropertiesCommand(table, properties, isView = true)

    case AlterTableUnsetPropertiesStatement(AsTableIdentifier(table), propertyKeys, ifExists) =>
      AlterTableUnsetPropertiesCommand(table, propertyKeys, ifExists, isView = false)

    case AlterViewUnsetPropertiesStatement(AsTableIdentifier(table), propertyKeys, ifExists) =>
      AlterTableUnsetPropertiesCommand(table, propertyKeys, ifExists, isView = true)

    case AlterTableSetLocationStatement(AsTableIdentifier(table), newLocation) =>
      AlterTableSetLocationCommand(table, None, newLocation)

    case AlterTableAddColumnsStatement(AsTableIdentifier(table), newColumns)
        if newColumns.forall(_.name.size == 1) =>
      // only top-level adds are supported using AlterTableAddColumnsCommand
      AlterTableAddColumnsCommand(table, newColumns.map(convertToStructField))

    case DeleteFromStatement(AsTableIdentifier(table), tableAlias, condition) =>
      throw new AnalysisException(
        s"Delete from tables is not supported using the legacy / v1 Spark external catalog" +
            s" API. Identifier: $table.")

    case ShowNamespacesStatement(None, pattern) if defaultCatalog.isEmpty =>
      ShowDatabasesCommand(pattern)

    case ShowNamespacesStatement(Some(CatalogAndRestNameParts(None, namespace)), _) =>
      throw new AnalysisException(
        s"No v2 catalog is available for ${namespace.quoted}")

    case ShowTablesStatement(None, pattern) =>
      defaultCatalog match {
        case Some(catalog) =>
          ShowTables(
            catalog.asTableCatalog,
            catalogManager.currentNamespace,
            pattern)
        case None =>
          ShowTablesCommand(None, pattern)
      }

    case ShowTablesStatement(Some(namespace), pattern) =>
      val CatalogNamespace(maybeCatalog, ns) = namespace
      maybeCatalog match {
        case Some(catalog) =>
          ShowTables(catalog.asTableCatalog, ns, pattern)
        case None =>
          if (namespace.length != 1) {
            throw new AnalysisException(
              s"The database name is not valid: ${namespace.quoted}")
          }
          ShowTablesCommand(Some(namespace.quoted), pattern)
      }
  }

  object V1Provider {
    def unapply(provider: String): Option[String] = {
      DataSource.lookupDataSourceV2(provider, conf) match {
        // TODO(SPARK-28396): Currently file source v2 can't work with tables.
        case Some(_: FileDataSourceV2) => Some(provider)
        case Some(_) => None
        case _ => Some(provider)
      }
    }
  }

  private def buildCatalogTable(
      table: TableIdentifier,
      schema: StructType,
      partitioning: Seq[Transform],
      bucketSpec: Option[BucketSpec],
      properties: Map[String, String],
      provider: String,
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      ifNotExists: Boolean): CatalogTable = {

    val storage = DataSource.buildStorageFormatFromOptions(options)
    if (location.isDefined && storage.locationUri.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
            "you can only specify one of them.")
    }
    val customLocation = storage.locationUri.orElse(location.map(CatalogUtils.stringToURI))

    val tableType = if (customLocation.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storage.copy(locationUri = customLocation),
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitioning.asPartitionColumns,
      bucketSpec = bucketSpec,
      properties = properties,
      comment = comment)
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
