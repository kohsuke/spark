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

package org.apache.spark.sql.sources.v2;

import java.util.Map;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalog.v2.expressions.Expression;
import org.apache.spark.sql.sources.Filter;

/**
 * A mix-in interface for {@link Table} update support. Data sources can implement this
 * interface to provide the ability to update data that matches filter expressions
 * with the given sets.
 */
@Experimental
public interface SupportsUpdate {

  /**
   * Update data that matches filter expressions with the given sets for a data source table.
   * <p>
   * Rows will be updated with the given values iff all of the filter expressions match.
   * That is, the expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Implementations may reject a update operation if the update isn't possible without significant
   * effort or it cannot deal with the sets expression. For example, partitioned data sources may
   * reject updates that do not filter by partition columns because the filter may require
   * rewriting files. The update may also be rejected if the update requires a complex computation
   * that the data source does not support.
   * To reject a delete implementations should throw {@link IllegalArgumentException} with a clear
   * error message that identifies which expression was rejected.
   *
   * @param sets the fields to be updated and the corresponding updated values in form of
   *             key-value pairs in a map. The value can be a literal, or a simple expression
   *             like {{{ originalValue + 1 }}}.
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @throws IllegalArgumentException If the update is rejected due to required effort
   *                                  or unsupported update expression
   */
  void updateWhere(Map<String, Expression> sets, Filter[] filters);
}
