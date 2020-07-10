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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.InsertableRelation

// TODO: fix name
case class WriteToDataSourceV2FallbackExec(
    relation: InsertableRelation,
    plan: LogicalPlan) extends V2CommandExec with SupportsV1Write {

  override def output: Seq[Attribute] = Nil
  override final def children: Seq[SparkPlan] = Nil

  override protected def run(): Seq[InternalRow] = {
    writeWithV1(relation)
  }
}

/**
 * A trait that allows Tables that use V1 Writer interfaces to append data.
 */
trait SupportsV1Write extends SparkPlan {
  // TODO: We should be able to work on SparkPlans at this point.
  def plan: LogicalPlan

  protected def writeWithV1(relation: InsertableRelation): Seq[InternalRow] = {
    relation.insert(Dataset.ofRows(sqlContext.sparkSession, plan), overwrite = false)
    Nil
  }
}
