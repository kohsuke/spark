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

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration) extends Logging {

  private val storeCpLocation = providerId.storeId.storeCheckpointLocation()
  private val fm = CheckpointFileManager.create(storeCpLocation, hadoopConf)
  private val schemaFileLocation = schemaFile(storeCpLocation)

  fm.mkdirs(schemaFileLocation.getParent)

  def check(keySchema: StructType, valueSchema: StructType): Unit = {
    if (fm.exists(schemaFileLocation)) {
      logDebug(s"Schema file for provider $providerId exists. Comparing with provided schema.")
      val (storedKeySchema, storedValueSchema) = readSchemaFile()

      def fieldToType: StructField => (DataType, Boolean) = f => (f.dataType, f.nullable)

      def typesEq(schema1: StructType, schema2: StructType): Boolean = {
        (schema1.length == schema2.length) && schema1.map(fieldToType) == schema2.map(fieldToType)
      }

      def namesEq(schema1: StructType, schema2: StructType): Boolean = {
        schema1.fieldNames.sameElements(schema2.fieldNames)
      }

      val errorMsg = "Provided schema doesn't match to the schema for existing state! " +
        "Please note that Spark allow difference of field name: check count of fields " +
        "and data type of each field.\n" +
        s"- provided schema: key $keySchema value $valueSchema\n" +
        s"- existing schema: key $storedKeySchema value $storedValueSchema\n" +
        s"If you want to force running query without schema validation, please set " +
        s"${SQLConf.STATE_SCHEMA_CHECK_ENABLED.key} to false."

      if (!typesEq(keySchema, storedKeySchema) || !typesEq(valueSchema, storedValueSchema)) {
        logError(errorMsg)
        throw new IllegalStateException(errorMsg)
      } else if (!namesEq(keySchema, storedKeySchema) ||
        !namesEq(valueSchema, storedValueSchema)) {
        logInfo("Detected the change of field name, will overwrite schema file to new.")
        // It tries best-effort to overwrite current schema file.
        // the schema validation doesn't break even it fails, though it might miss on detecting
        // change of field name which is not a big deal.
        createSchemaFile(keySchema, valueSchema)
      }
    } else {
      // schema doesn't exist, create one now
      logDebug(s"Schema file for provider $providerId doesn't exist. Creating one.")
      createSchemaFile(keySchema, valueSchema)
    }
  }

  private def readSchemaFile(): (StructType, StructType) = {
    val inStream = fm.open(schemaFileLocation)
    try {
      val keySchemaStr = inStream.readUTF()
      val valueSchemaStr = inStream.readUTF()

      (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
    } catch {
      case e: Throwable =>
        logError(s"Fail to read schema file from $schemaFileLocation", e)
        throw e
    } finally {
      inStream.close()
    }
  }

  private def createSchemaFile(keySchema: StructType, valueSchema: StructType): Unit = {
    val outStream = fm.createAtomic(schemaFileLocation, overwriteIfPossible = true)
    try {
      outStream.writeUTF(keySchema.json)
      outStream.writeUTF(valueSchema.json)
      outStream.close()
    } catch {
      case e: Throwable =>
        logError(s"Fail to write schema file to $schemaFileLocation", e)
        outStream.cancel()
        throw e
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}
