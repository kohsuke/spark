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

package org.apache.spark.sql.hive.client

import java.util.Collections

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A set of tests for the filter conversion logic used when pushing partition pruning into the
 * metastore
 */
class FiltersSuite extends SparkFunSuite with Logging with PlanTest {
  private val shim = new Shim_v0_13

  private val testTable = new org.apache.hadoop.hive.ql.metadata.Table("default", "test")
  private val varCharCol = new FieldSchema()
  varCharCol.setName("varchar")
  varCharCol.setType(serdeConstants.VARCHAR_TYPE_NAME)
  testTable.setPartCols(Collections.singletonList(varCharCol))

  filterTest("string filter",
    (a("stringcol", StringType) > Literal("test")) :: Nil,
    "stringcol > \"test\"")

  filterTest("string filter backwards",
    (Literal("test") > a("stringcol", StringType)) :: Nil,
    "\"test\" > stringcol")

  filterTest("int filter",
    (a("intcol", IntegerType) === Literal(1)) :: Nil,
    "intcol = 1")

  filterTest("int filter backwards",
    (Literal(1) === a("intcol", IntegerType)) :: Nil,
    "1 = intcol")

  filterTest("int and string filter",
    (Literal(1) === a("intcol", IntegerType)) :: (Literal("a") === a("strcol", StringType)) :: Nil,
    "1 = intcol and \"a\" = strcol")

  filterTest("int and int column/string literal filter",
    (a("intcol1", IntegerType) === Literal(1)) ::
      (a("intcol2", IntegerType) === (Literal("a"))) :: Nil,
    "intcol1 = 1")

  filterTest("int and int column/string literal filter with conversion",
    (a("intcol1", IntegerType) === Literal(1)) ::
      (a("intcol2", IntegerType) === (Literal("00002"))) :: Nil,
    "intcol1 = 1 and intcol2 = 2")

  filterTest("int and int column/string literal filter backwards",
    (Literal(1) === a("intcol1", IntegerType)) ::
      (Literal("a") === a("intcol2", IntegerType)) :: Nil,
    "1 = intcol1")

  filterTest("int and int column/string literal filter backwards with conversion",
    (Literal(1) === a("intcol1", IntegerType)) ::
      (Literal("00002") === a("intcol2", IntegerType)) :: Nil,
    "1 = intcol1 and 2 = intcol2")

  filterTest("int filter with in",
    (a("intcol", IntegerType) in (Literal(1), Literal(2))) :: Nil,
    "(intcol = 1 or intcol = 2)")

  filterTest("int/string filter with in",
    (a("intcol", IntegerType) in (Literal("1"), Literal("2"))) :: Nil,
    "")

  filterTest("int filter with inset",
    (a("intcol", IntegerType) in ((0 to 11).map(Literal(_)): _*)) :: Nil,
    "(" + (0 to 11).map(x => s"intcol = $x").mkString(" or ") + ")")

  filterTest("int/string filter with inset",
    (a("intcol", IntegerType) in ((0 to 11).map(x => Literal(x.toString)): _*)) :: Nil,
    "")

  filterTest("string filter with in",
    (a("strcol", StringType) in (Literal("1"), Literal("2"))) :: Nil,
    "(strcol = \"1\" or strcol = \"2\")")

  filterTest("skip varchar",
    (Literal("") === a("varchar", StringType)) :: Nil,
    "")

  filterTest("SPARK-19912 String literals should be escaped for Hive metastore partition pruning",
    (a("stringcol", StringType) === Literal("p1\" and q=\"q1")) ::
      (Literal("p2\" and q=\"q2") === a("stringcol", StringType)) :: Nil,
    """stringcol = 'p1" and q="q1' and 'p2" and q="q2' = stringcol""")

  filterTest("SPARK-24879 null literals should be ignored for IN constructs",
    (a("intcol", IntegerType) in (Literal(1), Literal(null))) :: Nil,
    "(intcol = 1)")

  // Applying the predicate `x IN (NULL)` should return an empty set, but since this optimization
  // will be applied by Catalyst, this filter converter does not need to account for this.
  filterTest("SPARK-24879 IN predicates with only NULLs will not cause a NPE",
    (a("intcol", IntegerType) in Literal(null)) :: Nil,
    "")

  filterTest("typecast null literals should not be pushed down in simple predicates",
    (a("intcol", IntegerType) === Literal(null, IntegerType)) :: Nil,
    "")

  private def filterTest(name: String, filters: Seq[Expression], result: String) = {
    test(name) {
      withSQLConf(SQLConf.ADVANCED_PARTITION_PREDICATE_PUSHDOWN.key -> "true") {
        val converted = shim.convertFilters(testTable, filters)
        if (converted != result) {
          fail(s"Expected ${filters.mkString(",")} to convert to '$result' but got '$converted'")
        }
      }
    }
  }

  test("turn on/off ADVANCED_PARTITION_PREDICATE_PUSHDOWN") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.ADVANCED_PARTITION_PREDICATE_PUSHDOWN.key -> enabled.toString) {
        val filters =
          (Literal(1) === a("intcol", IntegerType) ||
            Literal(2) === a("intcol", IntegerType)) :: Nil
        val converted = shim.convertFilters(testTable, filters)
        if (enabled) {
          assert(converted == "(1 = intcol or 2 = intcol)")
        } else {
          assert(converted.isEmpty)
        }
      }
    }
  }

  private def a(name: String, dataType: DataType) = AttributeReference(name, dataType)()
}
