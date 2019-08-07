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

package org.apache.spark.sql.catalog.v2;

import java.util.Map;

import org.apache.spark.sql.catalog.v2.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.types.StructType;

/**
 * An API to extend the Spark built-in catalog. Implementation can override some methods and add
 * hooks to apply some custom logic. For example, they can override {@code createTable}, and call
 * {@code super.createTable} with empty schema and partition, to avoid duplicating table metadata
 * in the Spark built-in catalog.
 *
 * Note that, the implementation must have a constructor that accepts a {@link TableCatalog}.
 */
public abstract class CatalogExtension implements TableCatalog {

  private final TableCatalog delegate;

  protected CatalogExtension(TableCatalog delegate) {
    this.delegate = delegate;
  }

  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return delegate.listTables(namespace);
  }

  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return delegate.loadTable(ident);
  }

  public void invalidateTable(Identifier ident) {
    delegate.invalidateTable(ident);
  }

  public boolean tableExists(Identifier ident) {
    return delegate.tableExists(ident);
  }

  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return delegate.createTable(ident, schema, partitions, properties);
  }

  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return delegate.alterTable(ident, changes);
  }

  public boolean dropTable(Identifier ident) {
    return delegate.dropTable(ident);
  }
}
