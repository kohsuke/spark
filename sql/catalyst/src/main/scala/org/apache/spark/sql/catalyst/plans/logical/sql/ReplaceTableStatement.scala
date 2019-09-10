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

import org.apache.spark.sql.catalog.v2.CatalogPlugin
import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTable, ReplaceTableAsSelect}
import org.apache.spark.sql.types.StructType

/**
 * A REPLACE TABLE command, as parsed from SQL.
 *
 * If the table exists prior to running this command, executing this statement
 * will replace the table's metadata and clear the underlying rows from the table.
 */
case class ReplaceTableStatement(
    tableName: Seq[String],
    tableSchema: StructType,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends StatementRequiringCatalog {

  override def nameParts: Seq[String] = tableName

  override def withCatalog(catalog: CatalogPlugin, restNameParts: Seq[String]): LogicalPlan = {
    ReplaceTable(
      catalog.asTableCatalog,
      restNameParts.toIdentifier,
      tableSchema,
      // convert the bucket spec and add it as a transform
      partitioning ++ bucketSpec.map(_.asTransform),
      CatalogV2Util.convertTableProperties(properties, options, location, comment, provider),
      orCreate)
  }
}

/**
 * A REPLACE TABLE AS SELECT command, as parsed from SQL.
 */
case class ReplaceTableAsSelectStatement(
    tableName: Seq[String],
    asSelect: LogicalPlan,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends StatementRequiringCatalog {

  override def children: Seq[LogicalPlan] = Seq(asSelect)

  override def nameParts: Seq[String] = tableName

  override def withCatalog(catalog: CatalogPlugin, restNameParts: Seq[String]): LogicalPlan = {
    ReplaceTableAsSelect(
      catalog.asTableCatalog,
      restNameParts.toIdentifier,
      // convert the bucket spec and add it as a transform
      partitioning ++ bucketSpec.map(_.asTransform),
      asSelect,
      CatalogV2Util.convertTableProperties(properties, options, location, comment, provider),
      writeOptions = options.filterKeys(_ != "path"),
      orCreate = orCreate)
  }
}
