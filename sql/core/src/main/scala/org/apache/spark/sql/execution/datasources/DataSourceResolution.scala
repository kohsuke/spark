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

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2._
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{CastSupport, DataSourceV2Helpers, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.sql._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileDataSourceV2}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.internal.V1Table
import org.apache.spark.sql.types._

case class DataSourceResolution(
    conf: SQLConf,
    catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with CastSupport with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
  import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util.loadTable

  def v2SessionCatalog: CatalogPlugin = sessionCatalog.getOrElse(
    throw new AnalysisException("No v2 session catalog implementation is available"))

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1Provider(provider), options, location, comment, ifNotExists) =>
      // the source is v1, the identifier has no catalog, and there is no default v2 catalog
      val tableDesc = buildCatalogTable(table, schema, partitionCols, bucketSpec, properties,
        provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, None)

    case create: CreateTableStatement =>
      // the provider was not a v1 source or a v2 catalog is the default, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      maybeCatalog match {
        case Some(catalog) =>
          // the identifier had a catalog, or there is a default v2 catalog
          convertCreateTable(catalog.asTableCatalog, identifier, create)
        case _ =>
          // the identifier had no catalog and no default catalog is set, but the source is v2.
          // use the v2 session catalog, which delegates to the global v1 session catalog
          convertCreateTable(v2SessionCatalog.asTableCatalog, identifier, create)
      }

    case CreateTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        V1Provider(provider), options, location, comment, ifNotExists) =>
      // the source is v1, the identifier has no catalog, and there is no default v2 catalog
      val tableDesc = buildCatalogTable(table, new StructType, partitionCols, bucketSpec,
        properties, provider, options, location, comment, ifNotExists)
      val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

      CreateTable(tableDesc, mode, Some(query))

    case create: CreateTableAsSelectStatement =>
      // the provider was not a v1 source or a v2 catalog is the default, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = create.tableName
      maybeCatalog match {
        case Some(catalog) =>
          // the identifier had a catalog, or there is a default v2 catalog
          convertCTAS(catalog.asTableCatalog, identifier, create)
        case _ =>
          // the identifier had no catalog and no default catalog is set, but the source is v2.
          // use the v2 session catalog, which delegates to the global v1 session catalog
          convertCTAS(v2SessionCatalog.asTableCatalog, identifier, create)
      }

    case DescribeColumnStatement(
        AsTableIdentifier(tableName), colName, isExtended) =>
      DescribeColumnCommand(tableName, colName, isExtended)

    case DescribeColumnStatement(
        CatalogObjectIdentifier(Some(catalog), ident), colName, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case DescribeTableStatement(
        AsTableIdentifier(tableName), partitionSpec, isExtended) =>
      DescribeTableCommand(tableName, partitionSpec, isExtended)

    case ReplaceTableStatement(
        AsTableIdentifier(table), schema, partitionCols, bucketSpec, properties,
        V1Provider(provider), options, location, comment, orCreate) =>
        throw new AnalysisException(
          s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
            s" API. Write provider name: $provider, identifier: $table.")

    case ReplaceTableAsSelectStatement(
        AsTableIdentifier(table), query, partitionCols, bucketSpec, properties,
        V1Provider(provider), options, location, comment, orCreate) =>
      throw new AnalysisException(
        s"Replacing tables is not supported using the legacy / v1 Spark external catalog" +
          s" API. Write provider name: $provider, identifier: $table.")

    case replace: ReplaceTableStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = replace.tableName
      val catalog = maybeCatalog.orElse(sessionCatalog)
        .getOrElse(throw new AnalysisException(
          s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
        .asTableCatalog
      convertReplaceTable(catalog, identifier, replace)

    case rtas: ReplaceTableAsSelectStatement =>
      // the provider was not a v1 source, convert to a v2 plan
      val CatalogObjectIdentifier(maybeCatalog, identifier) = rtas.tableName
      val catalog = maybeCatalog.orElse(sessionCatalog)
        .getOrElse(throw new AnalysisException(
          s"No catalog specified for table ${identifier.quoted} and no default catalog is set"))
        .asTableCatalog
      convertRTAS(catalog, identifier, rtas)

    case DropTableStatement(CatalogObjectIdentifier(Some(catalog), ident), ifExists, _) =>
      DropTable(catalog.asTableCatalog, ident, ifExists)

    case DropTableStatement(AsTableIdentifier(tableName), ifExists, purge) =>
      DropTableCommand(tableName, ifExists, isView = false, purge)

    case DropViewStatement(CatalogObjectIdentifier(Some(catalog), ident), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view $ident " +
          s"because view support in catalog has not been implemented yet")

    case DropViewStatement(AsTableIdentifier(tableName), ifExists) =>
      DropTableCommand(tableName, ifExists, isView = true, purge = false)

    case i @ InsertIntoStatement(UnresolvedRelation(
        CatalogObjectIdentifier(None, ident)), _, _, _, _) if i.query.resolved =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(v1: V1Table) =>
          InsertIntoTable(UnresolvedCatalogRelation(v1.v1Table),
            i.partitionSpec, i.query, i.overwrite, i.ifPartitionNotExists)
        case tableOpt =>
          tableOpt.map(DataSourceV2Helpers.resolveInsertInto(i, _, conf))
            .getOrElse(InsertIntoTable(
              i.table, i.partitionSpec, i.query, i.overwrite, i.ifPartitionNotExists))
      }

    case AlterTableAddColumnsStatement(CatalogObjectIdentifier(None, ident), cols) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(v1: V1Table) if cols.forall(_.name.size == 1) =>
          AlterTableAddColumnsCommand(v1.v1Table.identifier, cols.map(convertToStructField))
        case Some(_: V1Table) =>
          throw new AnalysisException("Adding nested columns is not supported for V1 tables.")
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = DataSourceV2Helpers.addColumnChanges(cols)
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableAlterColumnStatement(
        CatalogObjectIdentifier(None, ident), colName, dataType, comment) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(_: V1Table) =>
          throw new AnalysisException("ALTER COLUMN is not supported for V1 tables.")
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = DataSourceV2Helpers.alterColumnChanges(colName, dataType, comment)
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableRenameColumnStatement(CatalogObjectIdentifier(None, ident), col, newName) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(_: V1Table) =>
          throw new AnalysisException("RENAME COLUMN is not supported for V1 tables.")
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = Seq(TableChange.renameColumn(col.toArray, newName))
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableDropColumnsStatement(CatalogObjectIdentifier(None, ident), cols) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(_: V1Table) =>
          throw new AnalysisException("RENAME COLUMN is not supported for V1 tables.")
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableSetPropertiesStatement(CatalogObjectIdentifier(None, ident), props) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(v1: V1Table) =>
          AlterTableSetPropertiesCommand(v1.v1Table.identifier, props, isView = false)
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = props.map { case (key, value) => TableChange.setProperty(key, value) }.toSeq
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableUnsetPropertiesStatement(CatalogObjectIdentifier(None, ident), keys, ifExists) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(v1: V1Table) =>
          AlterTableUnsetPropertiesCommand(v1.v1Table.identifier, keys, ifExists, isView = false)
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = keys.map(key => TableChange.removeProperty(key))
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterTableSetLocationStatement(CatalogObjectIdentifier(None, ident), newLoc) =>
      loadTable(v2SessionCatalog, ident) match {
        case Some(v1: V1Table) =>
          AlterTableSetLocationCommand(v1.v1Table.identifier, None, newLoc)
        case tableOpt =>
          val relation = tableOpt.map(DataSourceV2Relation.create)
            .getOrElse(UnresolvedRelation(ident.namespace() :+ ident.name()))
          val changes = Seq(TableChange.setProperty("location", newLoc))
          AlterTable(v2SessionCatalog.asTableCatalog, ident, relation, changes)
      }

    case AlterViewSetPropertiesStatement(AsTableIdentifier(table), properties) =>
      AlterTableSetPropertiesCommand(table, properties, isView = true)

    case AlterViewUnsetPropertiesStatement(AsTableIdentifier(table), propertyKeys, ifExists) =>
      AlterTableUnsetPropertiesCommand(table, propertyKeys, ifExists, isView = true)

    case DeleteFromStatement(AsTableIdentifier(table), tableAlias, condition) =>
      throw new AnalysisException(
        s"Delete from tables is not supported using the legacy / v1 Spark external catalog" +
            s" API. Identifier: $table.")

    case delete: DeleteFromStatement =>
      val relation = UnresolvedRelation(delete.tableName)
      val aliased = delete.tableAlias.map(SubqueryAlias(_, relation)).getOrElse(relation)
      DeleteFromTable(aliased, delete.condition)

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

  private def convertCTAS(
      catalog: TableCatalog,
      identifier: Identifier,
      ctas: CreateTableAsSelectStatement): CreateTableAsSelect = {
    // convert the bucket spec and add it as a transform
    val partitioning = ctas.partitioning ++ ctas.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      ctas.properties, ctas.options, ctas.location, ctas.comment, ctas.provider)

    CreateTableAsSelect(
      catalog,
      identifier,
      partitioning,
      ctas.asSelect,
      properties,
      writeOptions = ctas.options.filterKeys(_ != "path"),
      ignoreIfExists = ctas.ifNotExists)
  }

  private def convertCreateTable(
      catalog: TableCatalog,
      identifier: Identifier,
      create: CreateTableStatement): CreateV2Table = {
    // convert the bucket spec and add it as a transform
    val partitioning = create.partitioning ++ create.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      create.properties, create.options, create.location, create.comment, create.provider)

    CreateV2Table(
      catalog,
      identifier,
      create.tableSchema,
      partitioning,
      properties,
      ignoreIfExists = create.ifNotExists)
  }

  private def convertRTAS(
      catalog: TableCatalog,
      identifier: Identifier,
      rtas: ReplaceTableAsSelectStatement): ReplaceTableAsSelect = {
    // convert the bucket spec and add it as a transform
    val partitioning = rtas.partitioning ++ rtas.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      rtas.properties, rtas.options, rtas.location, rtas.comment, rtas.provider)

    ReplaceTableAsSelect(
      catalog,
      identifier,
      partitioning,
      rtas.asSelect,
      properties,
      writeOptions = rtas.options.filterKeys(_ != "path"),
      orCreate = rtas.orCreate)
  }

  private def convertReplaceTable(
      catalog: TableCatalog,
      identifier: Identifier,
      replace: ReplaceTableStatement): ReplaceTable = {
    // convert the bucket spec and add it as a transform
    val partitioning = replace.partitioning ++ replace.bucketSpec.map(_.asTransform)
    val properties = convertTableProperties(
      replace.properties, replace.options, replace.location, replace.comment, replace.provider)

    ReplaceTable(
      catalog,
      identifier,
      replace.tableSchema,
      partitioning,
      properties,
      orCreate = replace.orCreate)
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
