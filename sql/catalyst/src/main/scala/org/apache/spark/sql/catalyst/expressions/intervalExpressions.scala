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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, LongType}
import org.apache.spark.unsafe.types.CalendarInterval

case class MultiplyInterval(interval: Expression, num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType, LongType)
  override def dataType: DataType = CalendarIntervalType

  override def nullable: Boolean = true

  override def nullSafeEval(interval: Any, num: Any): Any = {
    try {
      interval.asInstanceOf[CalendarInterval].multiply(num.asInstanceOf[Long])
    } catch {
      case _: java.lang.ArithmeticException => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (interval, num) => {
      s"""
        try {
          ${ev.value} = $interval.multiply($num);
        } catch (java.lang.ArithmeticException e) {
          ${ev.isNull} = true;
        }
      """
    })
  }
}
