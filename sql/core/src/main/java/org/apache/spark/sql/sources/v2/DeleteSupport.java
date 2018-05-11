/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.catalyst.expressions.Expression;

public interface DeleteSupport extends DataSourceV2 {
  /**
   * Delete data from a data source table that matches filter expressions.
   * <p>
   * Rows are deleted from the data source iff all of the filter expressions match. That is, the
   * expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Implementations may reject a delete operation if the delete isn't possible without significant
   * effort. For example, partitioned data sources may reject deletes that do not filter by
   * partition columns because the filter may require rewriting files without deleted records.
   * To reject a delete implementations should throw {@link IllegalArgumentException} with a clear
   * error message that identifies which expression was rejected.
   * <p>
   * Implementations may throw {@link UnsupportedOperationException} if the delete operation is not
   * supported because one of the filter expressions is not supported. Implementations should throw
   * this exception with a clear error message that identifies the unsupported expression.
   *
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @throws UnsupportedOperationException If one or more filter expressions is not supported
   * @throws IllegalArgumentException If the delete is rejected due to required effort
   */
  void deleteWhere(Expression[] filters);
}
