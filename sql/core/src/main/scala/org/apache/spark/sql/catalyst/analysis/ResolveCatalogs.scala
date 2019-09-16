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

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, DropTable, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, ShowNamespaces, ShowTables}
import org.apache.spark.sql.catalyst.plans.logical.sql.{CreateTableAsSelectStatement, CreateTableStatement, DropTableStatement, DropViewStatement, ReplaceTableAsSelectStatement, ReplaceTableStatement, ShowNamespacesStatement, ShowTablesStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.{DropTableCommand, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Resolves catalogs from the multi-part identifiers in DDL/DML commands.
 *
 * For each SQL statement, this rule has 2 different code paths for the session catalog and other
 * catalogs.
 */
class ResolveCatalogs(val catalogManager: CatalogManager, conf: SQLConf)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    // For CREATE TABLE and CTAS, we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is v1 source.

    case c: CreateTableStatement =>
      c.tableName match {
        case CatalogAndRestNameParts(Right(sessionCatalog), _) if isV1Provider(c.provider) =>
          val tableDesc = buildCatalogTable(c.tableName.toV1Identifier, c.schema, c.partitioning,
            c.bucketSpec, c.properties, c.provider, c.options, c.location, c.comment, c.ifNotExists)
          val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTable(tableDesc, mode, None)

        case _ =>
          val (catalog, ident) = c.tableName match {
            case CatalogAndRestNameParts(Left(cata), restNameParts) =>
              cata -> restNameParts.toIdentifier
            case CatalogAndRestNameParts(Right(cata), restNameParts) =>
              cata -> restNameParts.toIdentifier
          }
          CreateV2Table(
            catalog.asTableCatalog,
            ident,
            c.tableSchema,
            // convert the bucket spec and add it as a transform
            c.partitioning ++ c.bucketSpec.map(_.asTransform),
            convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
            ignoreIfExists = c.ifNotExists)
      }

    case c: CreateTableAsSelectStatement =>
      c.tableName match {
        case CatalogAndRestNameParts(Right(sessionCatalog), _) if isV1Provider(c.provider) =>
          val tableDesc = buildCatalogTable(c.tableName.toV1Identifier, c.schema, c.partitioning,
            c.bucketSpec, c.properties, c.provider, c.options, c.location, c.comment, c.ifNotExists)
          val mode = if (c.ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTable(tableDesc, mode, Some(c.asSelect))

        case _ =>
          val (catalog, ident) = c.tableName match {
            case CatalogAndRestNameParts(Left(cata), restNameParts) =>
              cata -> restNameParts.toIdentifier
            case CatalogAndRestNameParts(Right(cata), restNameParts) =>
              cata -> restNameParts.toIdentifier
          }
          CreateTableAsSelect(
            catalog.asTableCatalog,
            ident,
            // convert the bucket spec and add it as a transform
            c.partitioning ++ c.bucketSpec.map(_.asTransform),
            c.asSelect,
            convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
            writeOptions = c.options.filterKeys(_ != "path"),
            ignoreIfExists = c.ifNotExists)
      }

    case c @ ReplaceTableStatement(
         CatalogAndRestNameParts(Left(catalog), restNameParts), _, _, _, _, _, _, _, _, _) =>
      ReplaceTable(
        catalog.asTableCatalog,
        restNameParts.toIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        orCreate = c.orCreate)

    case c @ ReplaceTableStatement(
         CatalogAndRestNameParts(Right(sessionCatalog), restNameParts),
         _, _, _, _, _, _, _, _, _) =>
      throw new AnalysisException(
        "REPLACE TABLE is not supported with the session catalog.")

    case c @ ReplaceTableAsSelectStatement(
         CatalogAndRestNameParts(Left(catalog), restNameParts), _, _, _, _, _, _, _, _, _) =>
      ReplaceTableAsSelect(
        catalog.asTableCatalog,
        restNameParts.toIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options.filterKeys(_ != "path"),
        orCreate = c.orCreate)

    case c @ ReplaceTableAsSelectStatement(
         CatalogAndRestNameParts(Right(sessionCatalog), restNameParts),
         _, _, _, _, _, _, _, _, _) =>
      throw new AnalysisException(
        "REPLACE TABLE AS SELECT is not supported with the session catalog.")

    case DropTableStatement(
         CatalogAndRestNameParts(Left(catalog), restNameParts), ifExists, purge) =>
      DropTable(catalog.asTableCatalog, restNameParts.toIdentifier, ifExists)

    case d @ DropTableStatement(
         CatalogAndRestNameParts(Right(sessionCatalog), restNameParts), ifExists, purge) =>
      DropTableCommand(d.tableName.toV1Identifier, ifExists, isView = false, purge = purge)

    case DropViewStatement(
         CatalogAndRestNameParts(Left(catalog), restNameParts), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${restNameParts.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case DropViewStatement(
         CatalogAndRestNameParts(Right(sessionCatalog), restNameParts), ifExists) =>
      DropTableCommand(restNameParts.toV1Identifier, ifExists, isView = true, purge = false)

    case ShowNamespacesStatement(
         Some(CatalogAndRestNameParts(Left(catalog), restNameParts)), pattern) =>
      val namespace = if (restNameParts.isEmpty) None else Some(restNameParts)
      ShowNamespaces(catalog.asNamespaceCatalog, namespace, pattern)

    case ShowNamespacesStatement(
         Some(CatalogAndRestNameParts(Right(sessionCatalog), restNameParts)), pattern) =>
      throw new AnalysisException(
        "SHOW NAMESPACES is not supported with the session catalog.")

    case ShowNamespacesStatement(None, pattern) =>
      if (defaultCatalog.isDefined) {
        ShowNamespaces(defaultCatalog.get.asNamespaceCatalog, None, pattern)
      } else {
        throw new AnalysisException(
          "SHOW NAMESPACES is not supported with the session catalog.")
      }

    case ShowTablesStatement(
         Some(CatalogAndRestNameParts(Left(catalog), restNameParts)), pattern) =>
      ShowTables(catalog.asTableCatalog, restNameParts, pattern)

    case ShowTablesStatement(
         Some(CatalogAndRestNameParts(Right(sessionCatalog), restNameParts)), pattern) =>
      if (restNameParts.length != 1) {
        throw new AnalysisException(
          s"The database name is not valid: ${restNameParts.quoted}")
      }
      ShowTablesCommand(Some(restNameParts.head), pattern)

    case ShowTablesStatement(None, pattern) =>
      if (defaultCatalog.isDefined) {
        ShowTables(defaultCatalog.get.asTableCatalog, catalogManager.currentNamespace, pattern)
      } else {
        ShowTablesCommand(None, pattern)
      }
  }

  private def convertTableProperties(
      properties: Map[String, String],
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      provider: String): Map[String, String] = {
    if (options.contains("path") && location.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
          "you can only specify one of them.")
    }

    if ((options.contains("comment") || properties.contains("comment"))
      && comment.isDefined) {
      throw new AnalysisException(
        "COMMENT and option/property 'comment' are both used to set the table comment, you can " +
          "only specify one of them.")
    }

    if (options.contains("provider") || properties.contains("provider")) {
      throw new AnalysisException(
        "USING and option/property 'provider' are both used to set the provider implementation, " +
          "you can only specify one of them.")
    }

    val filteredOptions = options.filterKeys(_ != "path")

    // create table properties from TBLPROPERTIES and OPTIONS clauses
    val tableProperties = new mutable.HashMap[String, String]()
    tableProperties ++= properties
    tableProperties ++= filteredOptions

    // convert USING, LOCATION, and COMMENT clauses to table properties
    tableProperties += ("provider" -> provider)
    comment.map(text => tableProperties += ("comment" -> text))
    location.orElse(options.get("path")).map(loc => tableProperties += ("location" -> loc))

    tableProperties.toMap
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

  private def isV1Provider(provider: String): Boolean = {
    DataSource.lookupDataSourceV2(provider, conf) match {
      // TODO(SPARK-28396): Currently file source v2 can't work with tables.
      case Some(_: FileDataSourceV2) => true
      case Some(_) => false
      case _ => true
    }
  }
}
