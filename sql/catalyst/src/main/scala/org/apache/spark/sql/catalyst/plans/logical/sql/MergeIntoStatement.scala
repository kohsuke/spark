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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class MergeIntoStatement(
    targetTableName: Seq[String],
    targetTableAlias: Option[String],
    sourceTableName: Option[Seq[String]],
    sourceQuery: Option[LogicalPlan],
    sourceTableAlias: Option[String],
    mergeCondition: Expression,
    matchedClauses: Seq[MergeClause],
    notMatchedClauses: Seq[MergeClause]) extends ParsedStatement

sealed abstract class MergeClause(condition: Option[Expression])

case class DeleteClause(deleteCondition: Option[Expression]) extends MergeClause(deleteCondition)

case class UpdateClause(updateCondition: Option[Expression],
    columns: Seq[Seq[String]],
    values: Seq[Expression]) extends MergeClause(updateCondition)

case class InsertClause(insertCondition: Option[Expression],
    columns: Seq[Seq[String]],
    values: Seq[Expression]) extends MergeClause(insertCondition)
