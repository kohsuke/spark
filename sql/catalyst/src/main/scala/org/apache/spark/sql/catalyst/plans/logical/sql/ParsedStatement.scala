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

import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.v2.Table

/**
 * A logical plan node that contains exactly what was parsed from SQL.
 *
 * This is used to hold information parsed from SQL when there are multiple implementations of a
 * query or command. For example, CREATE TABLE may be implemented by different nodes for v1 and v2.
 * Instead of parsing directly to a v1 CreateTable that keeps metadata in CatalogTable, and then
 * converting that v1 metadata to the v2 equivalent, the sql [[CreateTableStatement]] plan is
 * produced by the parser and converted once into both implementations.
 *
 * Parsed logical plans are not resolved because they must be converted to concrete logical plans.
 *
 * Parsed logical plans are located in Catalyst so that as much SQL parsing logic as possible is be
 * kept in a [[org.apache.spark.sql.catalyst.parser.AbstractSqlParser]].
 */
abstract class ParsedStatement extends LogicalPlan {
  // Redact properties and options when parsed nodes are used by generic methods like toString
  override def productIterator: Iterator[Any] = super.productIterator.map {
    case mapArg: Map[_, _] => conf.redactOptions(mapArg)
    case other => other
  }

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  final override lazy val resolved = false
}

/**
 * A special [[ParsedStatement]] which needs to look up the catalog to further resolve itself.
 */
abstract class StatementRequiringCatalog extends ParsedStatement {
  def nameParts: Seq[String]

  // TODO: this method should be called with session catalog as well. However, for now we still
  //       need to fallback to v1 command in some cases. The `StatementRequiringCatalog` will
  //       remain unchanged if session catalog is picked, and wait for other analyzer rules to
  //       convert it to v1 command.
  def withCatalog(catalog: CatalogPlugin, restNameParts: Seq[String]): LogicalPlan
}

/**
 * A special [[ParsedStatement]] which needs to look up the catalog and table to further resolve
 * itself.
 */
abstract class StatementRequiringCatalogAndTable extends ParsedStatement {
  def tableName: Seq[String]

  // This method will not be called with `V1Table`. Extra analyzer rules are needed to resolve
  // `StatementRequiringCatalogAndTable` with `V1Table`. For example, the `DESC TABLE` has
  // different output for v1 and v2 tables.
  def withCatalogAndTable(catalog: TableCatalog, ident: Identifier, table: Table): LogicalPlan
}
