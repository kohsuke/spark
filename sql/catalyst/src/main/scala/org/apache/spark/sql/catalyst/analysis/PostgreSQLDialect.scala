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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.postgreSQL.PostgreCastStringToBoolean
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType}

object PostgreSQLDialect {
  def postgreSQLDialectRules(conf: SQLConf): List[Rule[LogicalPlan]] =
    if (conf.usePostgreSQLDialect) {
      postgreCastStringToBoolean(conf) ::
        Nil
    } else {
      Nil
    }

  case class postgreCastStringToBoolean(conf: SQLConf) extends Rule[LogicalPlan] with Logging {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformExpressions {
        case Cast(child, dataType, _) if dataType == BooleanType && child.dataType == StringType =>
          PostgreCastStringToBoolean(child)
      }
    }
  }
}
