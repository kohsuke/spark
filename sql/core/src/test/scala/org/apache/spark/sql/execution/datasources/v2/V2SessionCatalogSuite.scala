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

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.{Catalogs, Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{Table, TableProvider}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2SessionCatalogSuite
    extends SparkFunSuite with SharedSQLContext with BeforeAndAfter with BeforeAndAfterAll {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val schema: StructType = new StructType()
      .add("id", IntegerType)
      .add("data", StringType)
  private val v2Source = classOf[FakeV2Provider].getName

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("""CREATE DATABASE IF NOT EXISTS db""")
    spark.sql("""CREATE DATABASE IF NOT EXISTS ns""")
    spark.sql("""CREATE DATABASE IF NOT EXISTS ns2""")
  }

  override protected def afterAll(): Unit = {
    spark.sql("""DROP TABLE IF EXISTS db.test_table""")
    spark.sql("""DROP DATABASE IF EXISTS db""")
    spark.sql("""DROP DATABASE IF EXISTS ns""")
    spark.sql("""DROP DATABASE IF EXISTS ns2""")
    super.afterAll()
  }

  after {
    newCatalog().dropTable(testIdent)
  }

  private def newCatalog(): TableCatalog = {
    val newCatalog = new V2SessionCatalog(spark.sessionState)
    newCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val testIdent = Identifier.of(Array("db"), "test_table")

  test("Catalogs can load the catalog") {
    val catalog = newCatalog()

    val conf = new SQLConf
    conf.setConfString("spark.sql.catalog.test", catalog.getClass.getName)

    val loaded = Catalogs.load("test", conf)
    assert(loaded.getClass == catalog.getClass)
  }

  test("listTables") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns"), "test_table_1")
    val ident2 = Identifier.of(Array("ns"), "test_table_2")
    val ident3 = Identifier.of(Array("ns2"), "test_table_1")

    assert(catalog.listTables(Array("ns")).isEmpty)

    catalog.createTable(ident1, schema, Array.empty, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1))
    assert(catalog.listTables(Array("ns2")).isEmpty)

    catalog.createTable(ident3, schema, Array.empty, emptyProps)
    catalog.createTable(ident2, schema, Array.empty, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1, ident2))
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident1)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident2))

    catalog.dropTable(ident2)

    assert(catalog.listTables(Array("ns")).isEmpty)
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident3)
  }

  test("createTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(table.properties.asScala == Map())

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: with properties") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("property", "value")

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(table.properties == properties)

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: table already exists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    val exc = intercept[TableAlreadyExistsException] {
      catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    }

    assert(exc.message.contains(table.name()))
    assert(exc.message.contains("already exists"))

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: duplicate column names in the table definition") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val errMsg = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT, $c1 INT) USING $v2Source")
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the table definition of t"))
      }
    }
  }

  test("createTable: partition column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE tbl(a int, b string) USING $v2Source PARTITIONED BY (c)")
    }.getMessage
    assert(e.contains("Couldn't find column c in"))
  }

  test("createTable: bucket column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE tbl(a int, b string) " +
        s"USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS")
    }.getMessage
    assert(e.contains("Couldn't find column c in"))
  }

  test("createTable: column repeated in partition columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val e = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)")
        }.getMessage
        assert(e.contains("Found duplicate column(s) in the partition schema"))
      }
    }
  }

  test("createTable: column repeated in bucket columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val e = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS")
        }.getMessage
        assert(e.contains("Found duplicate column(s) in the bucket definition"))
      }
    }
  }

  test("createTable: all columns used in partitioning") {
    Seq(
      "PARTITIONED BY (a, b)",
      "CLUSTERED BY (a, b) INTO 2 BUCKETS",
      "PARTITIONED BY (a) CLUSTERED BY (b) INTO 2 BUCKETS").foreach { partitioning =>

      val e = intercept[AnalysisException] {
        sql(s"CREATE TABLE t(a INT, b STRING) USING $v2Source $partitioning")
      }.getMessage
      assert(e.contains("Cannot use all columns for partitioning."))
    }
  }

  test("tableExists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))

    catalog.dropTable(testIdent)

    assert(!catalog.tableExists(testIdent))
  }

  test("loadTable") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
  }

  test("loadTable: table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.loadTable(testIdent)
    }

    assert(exc.message.contains(testIdent.quoted))
    assert(exc.message.contains("not found"))
  }

  test("invalidateTable") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    catalog.invalidateTable(testIdent)

    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
  }

  test("invalidateTable: table does not exist") {
    val catalog = newCatalog()

    assert(catalog.tableExists(testIdent) === false)

    catalog.invalidateTable(testIdent)
  }

  test("alterTable: add property") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.properties.asScala == Map())

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-1", "1"))
    assert(updated.properties.asScala == Map("prop-1" -> "1"))

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map("prop-1" -> "1"))

    assert(table.properties.asScala == Map())
  }

  test("alterTable: add property to existing") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    assert(table.properties.asScala == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-2", "2"))
    assert(updated.properties.asScala == Map("prop-1" -> "1", "prop-2" -> "2"))

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map("prop-1" -> "1", "prop-2" -> "2"))

    assert(table.properties.asScala == Map("prop-1" -> "1"))
  }

  test("alterTable: remove existing property") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    assert(table.properties.asScala == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(updated.properties.asScala == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map())

    assert(table.properties.asScala == Map("prop-1" -> "1"))
  }

  test("alterTable: remove missing property") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.properties.asScala == Map())

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(updated.properties.asScala == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map())

    assert(table.properties.asScala == Map())
  }

  test("alterTable: add top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.addColumn(Array("ts"), TimestampType))

    assert(updated.schema == schema.add("ts", TimestampType))
  }

  test("alterTable: add required column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false))

    assert(updated.schema == schema.add("ts", TimestampType, nullable = false))
  }

  test("alterTable: add column with comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false, "comment text"))

    val field = StructField("ts", TimestampType, nullable = false).withComment("comment text")
    assert(updated.schema == schema.add(field))
  }

  test("alterTable: add nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("point", "z"), DoubleType))

    val expectedSchema = schema.add("point", pointStruct.add("z", DoubleType))

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: add column to primitive field fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.addColumn(Array("data", "ts"), TimestampType))
    }

    assert(exc.getMessage.contains("Not a struct"))
    assert(exc.getMessage.contains("data"))

    // the table has not changed
    assert(catalog.loadTable(testIdent).schema == schema)
  }


  test("alterTable add columns with existing column name") {
    val catalog = newCatalog()

    val testSchema = new StructType().add("foo", "int")
    val table = catalog.createTable(testIdent, testSchema, Array.empty, emptyProps)
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      Seq("foo", "fOo", "FOO").foreach { name =>
        val e = intercept[AnalysisException] {
          val tc = TableChange.addColumn(Array(name), LongType)
          catalog.alterTable(testIdent, tc)
        }.getMessage
        assert(e.contains("Found duplicate column(s)"))
      }
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val e = intercept[AnalysisException] {
        val tc = TableChange.addColumn(Array("foo"), LongType)
        catalog.alterTable(testIdent, tc)
      }.getMessage
      assert(e.contains("Found duplicate column(s)"))

      Seq("fOo", "FOO").foreach { name =>
       val tc = TableChange.addColumn(Array(name), LongType)
        catalog.alterTable(testIdent, tc)
      }
    }
  }

  test("alterTable: add field to missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.addColumn(Array("missing_col", "new_field"), StringType))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: update column data type") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.updateColumnType(Array("id"), LongType))

    val expectedSchema = new StructType().add("id", LongType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update column data type and nullability") {
    val catalog = newCatalog()

    val originalSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("data", StringType)
    val table = catalog.createTable(testIdent, originalSchema, Array.empty, emptyProps)

    assert(table.schema == originalSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnType(Array("id"), LongType, true))

    val expectedSchema = new StructType().add("id", LongType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update optional column to required fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.updateColumnType(Array("id"), LongType, false))
    }

    assert(exc.getMessage.contains("Cannot change optional column to required"))
    assert(exc.getMessage.contains("id"))
  }

  test("alterTable: update missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.updateColumnType(Array("missing_col"), LongType))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: add comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnComment(Array("id"), "comment text"))

    val expectedSchema = new StructType()
        .add("id", IntegerType, nullable = true, "comment text")
        .add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: replace comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    catalog.alterTable(testIdent, TableChange.updateColumnComment(Array("id"), "comment text"))

    val expectedSchema = new StructType()
        .add("id", IntegerType, nullable = true, "replacement comment")
        .add("data", StringType)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnComment(Array("id"), "replacement comment"))

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: add comment to missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.updateColumnComment(Array("missing_col"), "comment"))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: rename top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.renameColumn(Array("id"), "some_id"))

    val expectedSchema = new StructType().add("some_id", IntegerType).add("data", StringType)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"))

    val newPointStruct = new StructType().add("first", DoubleType).add("y", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename struct column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point"), "p"))

    val newPointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val expectedSchema = schema.add("p", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.renameColumn(Array("missing_col"), "new_name"))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: multiple changes") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"),
      TableChange.renameColumn(Array("point", "y"), "second"))

    val newPointStruct = new StructType().add("first", DoubleType).add("second", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("id")))

    val expectedSchema = new StructType().add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("point", "y")))

    val newPointStruct = new StructType().add("x", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.deleteColumn(Array("missing_col")))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: delete missing nested column fails") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.deleteColumn(Array("point", "z")))
    }

    assert(exc.getMessage.contains("z"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.alterTable(testIdent, TableChange.setProperty("prop", "val"))
    }

    assert(exc.message.contains(testIdent.quoted))
    assert(exc.message.contains("not found"))
  }

  test("dropTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))

    val wasDropped = catalog.dropTable(testIdent)

    assert(wasDropped)
    assert(!catalog.tableExists(testIdent))
  }

  test("dropTable: table does not exist") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val wasDropped = catalog.dropTable(testIdent)

    assert(!wasDropped)
    assert(!catalog.tableExists(testIdent))
  }
}

/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("Unnecessary for DDL tests")
  }
}
