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

package org.apache.spark.sql.connector.catalog;

import java.util.Map;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A mix-in interface for {@link TableProvider}, to provide the ability of accepting external table
 * metadata when getting tables. The external table metadata includes user-specified schema from
 * `DataFrameReader`/`DataFrameWriter`, previously inferred schema/partitioning stored in Spark
 * catalog, etc.
 */
public interface SupportsExternalMetadata extends TableProvider {

  @Override
  default Table getTable(Map<String, String> properties) {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
    StructType schema = inferSchema(options);
    Transform[] partitioning = inferPartitioning(options);
    return getTable(schema, partitioning, properties);
  }

  /**
   * Infer the schema of the table identified by the given options.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *                e.g. file path, Kafka topic name, etc.
   */
  StructType inferSchema(CaseInsensitiveStringMap options);

  /**
   * Infer the partitioning of the table identified by the given options.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *                e.g. file path, Kafka topic name, etc.
   */
  Transform[] inferPartitioning(CaseInsensitiveStringMap options);

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   */
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);
}
