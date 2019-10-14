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

package org.apache.spark.graph.api

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{Dataset, Row}

/**
 * Describes how to map a Dataset to nodes.
 *
 * Each row in the Dataset represents a node which has exactly the labels defined by the given
 * label set.
 *
 * @param ds              Dataset containing a single node in each row
 * @param idColumn        column that contains the node identifier
 * @param labelSet        labels that are assigned to all nodes
 * @param propertyColumns mapping from property keys to corresponding columns
 * @since 3.0.0
 */
@Evolving
case class NodeDataset private[graph](
    ds: Dataset[Row],
    idColumn: String,
    labelSet: Set[String],
    propertyColumns: Map[String, String])
  extends GraphElementDataset
