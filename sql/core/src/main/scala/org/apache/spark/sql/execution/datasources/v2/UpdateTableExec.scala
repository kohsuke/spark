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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.expressions.Expression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.SupportsUpdate


case class UpdateTableExec(
    table: SupportsUpdate,
    attrs: Seq[String],
    values: Seq[Expression],
    condition: Array[Filter]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    try {
      table.updateWhere(attrs.zip(values).toMap.asJava, condition)
    } catch {
      case e: IllegalArgumentException =>
        throw new SparkException(s"Update table failed: ${e.getMessage}", e)
    }

    sparkContext.emptyRDD
  }

  override def output: Seq[Attribute] = Nil
}
