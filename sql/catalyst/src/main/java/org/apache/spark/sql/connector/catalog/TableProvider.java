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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The base interface for v2 data sources which don't have a real catalog. Implementations must
 * have a public, 0-arg constructor.
 * <p>
 * Note that, TableProvider can only apply data operations to existing tables, like read, append,
 * delete, and overwrite. It does not support the operations that require metadata changes, like
 * create/drop tables.
 * <p>
 * The major responsibility of this interface is to return a {@link Table} for read/write.
 * </p>
 */
@Evolving
public interface TableProvider {

  /**
   * Return a {@link Table} instance with the given table options to do read/write.
   * Implementations should infer the table schema and partitioning.
   *
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  // TODO: this should take a Map<String, String> as table properties.
  Table getTable(CaseInsensitiveStringMap options);
}
