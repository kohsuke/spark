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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * A proxy for an catalyst `Expression`. Given a runtime object `EvaluationRunTime`, when this
 * is asked to evaluate, it will load the evaluation cache in the runtime first.
 */
case class ExpressionProxy(child: Expression, runtime: EvaluationRunTime) extends Expression {

  final override def dataType: DataType = child.dataType
  final override def nullable: Boolean = child.nullable
  final override def children: Seq[Expression] = child :: Nil

  // `ExpressionProxy` is for interpreted expression evaluation only. So cannot `doGenCode`.
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

  def proxyEval(input: InternalRow = null): Any = {
    child.eval(input)
  }

  override def eval(input: InternalRow = null): Any = {
    runtime.cache.get(this).result
  }
}

/**
 * A simple wrapper for holding `Any` in the cache of `EvaluationRunTime`.
 */
case class ResultProxy(result: Any)
