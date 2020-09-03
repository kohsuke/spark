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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType.fromDDL

class StructTypeSuite extends SparkFunSuite {

  private val s = StructType.fromDDL("a INT, b STRING")

  test("lookup a single missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s("c")).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("lookup a set of missing fields should output existing fields") {
    val e = intercept[IllegalArgumentException](s(Set("a", "c"))).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("lookup fieldIndex for missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s.fieldIndex("c")).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("SPARK-24849: toDDL - simple struct") {
    val struct = StructType(Seq(StructField("a", IntegerType)))

    assert(struct.toDDL == "`a` INT")
  }

  test("SPARK-24849: round trip toDDL - fromDDL") {
    val struct = new StructType().add("a", IntegerType).add("b", StringType)

    assert(fromDDL(struct.toDDL) === struct)
  }

  test("SPARK-24849: round trip fromDDL - toDDL") {
    val struct = "`a` MAP<INT, STRING>,`b` INT"

    assert(fromDDL(struct).toDDL === struct)
  }

  test("SPARK-24849: toDDL must take into account case of fields.") {
    val struct = new StructType()
      .add("metaData", new StructType().add("eventId", StringType))

    assert(struct.toDDL == "`metaData` STRUCT<`eventId`: STRING>")
  }

  test("SPARK-24849: toDDL should output field's comment") {
    val struct = StructType(Seq(
      StructField("b", BooleanType).withComment("Field's comment")))

    assert(struct.toDDL == """`b` BOOLEAN COMMENT 'Field\'s comment'""")
  }


  test("Print up to the given level") {
    val schema = StructType.fromDDL(
      "c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")

    assert(5 == schema.treeString(2).split("\n").length)
    assert(3 == schema.treeString(1).split("\n").length)
    assert(7 == schema.treeString.split("\n").length)
    assert(7 == schema.treeString(0).split("\n").length)
    assert(7 == schema.treeString(-1).split("\n").length)

    val schema2 = StructType.fromDDL(
      "c1 INT, c2 ARRAY<STRUCT<c3: INT>>, c4 STRUCT<c5: INT, c6: ARRAY<ARRAY<INT>>>")
    assert(4 == schema2.treeString(1).split("\n").length)
    assert(7 == schema2.treeString(2).split("\n").length)
    assert(9 == schema2.treeString(3).split("\n").length)
    assert(10 == schema2.treeString(4).split("\n").length)
    assert(10 == schema2.treeString(0).split("\n").length)

    val schema3 = StructType.fromDDL(
      "c1 MAP<INT, STRUCT<c2: MAP<INT, INT>>>, c3 STRUCT<c4: MAP<INT, MAP<INT, INT>>>")
    assert(3 == schema3.treeString(1).split("\n").length)
    assert(6 == schema3.treeString(2).split("\n").length)
    assert(9 == schema3.treeString(3).split("\n").length)
    assert(13 == schema3.treeString(4).split("\n").length)
    assert(13 == schema3.treeString(0).split("\n").length)
  }

  test("interval keyword in schema string") {
    val interval = "`a` INTERVAL"
    assert(fromDDL(interval).toDDL === interval)
  }

  test("find missing (nested) fields") {
    val schema = StructType.fromDDL(
      "c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    val resolver = SQLConf.get.resolver

    val source1 = StructType.fromDDL("c1 INT")
    val missing1 = StructType.fromDDL(
      "c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source1, schema, resolver)
      .map(_.sameType(missing1)).getOrElse(false))

    val source2 = StructType.fromDDL("c1 INT, c3 STRING")
    val missing2 = StructType.fromDDL(
      "c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source2, schema, resolver)
      .map(_.sameType(missing2)).getOrElse(false))

    val source3 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT>")
    val missing3 = StructType.fromDDL(
      "c2 STRUCT<c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source3, schema, resolver)
      .map(_.sameType(missing3)).getOrElse(false))

    val source4 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c6: INT>>")
    val missing4 = StructType.fromDDL(
      "c2 STRUCT<c4: STRUCT<c5: INT>>")
    assert(StructType.findMissingFields(source4, schema, resolver)
      .map(_.sameType(missing4)).getOrElse(false))

    val schemaWithArray = StructType.fromDDL(
      "c1 INT, c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    val source5 = StructType.fromDDL(
      "c1 INT")
    val missing5 = StructType.fromDDL(
      "c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source5, schemaWithArray, resolver)
        .map(_.sameType(missing5)).getOrElse(false))

    val schemaWithMap1 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>, c3 LONG")
    val source6 = StructType.fromDDL(
      "c1 INT, c3 LONG")
    val missing6 = StructType.fromDDL(
      "c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>")
    assert(
      StructType.findMissingFields(source6, schemaWithMap1, resolver)
        .map(_.sameType(missing6)).getOrElse(false))

    val schemaWithMap2 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>, c3 STRING")
    val source7 = StructType.fromDDL(
      "c1 INT, c3 STRING")
    val missing7 = StructType.fromDDL(
      "c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source7, schemaWithMap2, resolver)
        .map(_.sameType(missing7)).getOrElse(false))

    // Unsupported: nested struct in array, map
    val source8 = StructType.fromDDL(
      "c1 INT, c2 ARRAY<STRUCT<c3: INT>>")
    // `findMissingFields` doesn't support looking into nested struct in array type.
    assert(StructType.findMissingFields(source8, schemaWithArray, resolver).isEmpty)

    val source9 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRUCT<c3: INT>, STRING>, c3 LONG")
    // `findMissingFields` doesn't support looking into nested struct in map type.
    assert(StructType.findMissingFields(source9, schemaWithMap1, resolver).isEmpty)

    val source10 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRING, STRUCT<c3: INT>>, c3 STRING")
    // `findMissingFields` doesn't support looking into nested struct in map type.
    assert(StructType.findMissingFields(source10, schemaWithMap2, resolver).isEmpty)
  }
}
