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

import java.io.Serializable;

/**
 * Interface for an aggregate function that can be partially applied in parallel tasks, then merged
 * to produce a final result.
 * <p>
 * Intermediate aggregation state must be {@link Serializable} so that state produced by parallel
 * tasks can be sent to a single executor and merged to produce a final result.
 *
 * @param <S> the JVM type for the aggregation's intermediate state; must be {@link Serializable}
 * @param <R> the JVM type of result values
 */
public interface AssociativeAggregateFunction<S extends Serializable, R>
    extends AggregateFunction<S, R> {

  /**
   * Merge two partial aggregation states.
   * <p>
   * This is called to merge intermediate aggregation states that were produced by parallel tasks.
   *
   * @param leftState intermediate aggregation state
   * @param rightState intermediate aggregation state
   * @return combined aggregation state
   */
  S merge(S leftState, S rightState);

}
