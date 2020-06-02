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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Base trait for state manager purposed to be used from streaming aggregations.
 */
sealed trait StreamingAggregationStateManager extends Serializable {

  /** Extract columns consisting key from input row, and return the new row for key columns. */
  def getKey(row: UnsafeRow): UnsafeRow

  /** Calculate schema for the value of state. The schema is mainly passed to the StateStoreRDD. */
  def getStateValueSchema: StructType

  /** Get the current value of a non-null key from the target state store. */
  def get(store: StateStore, key: UnsafeRow): UnsafeRow

  /**
   * Put a new value for a non-null key to the target state store. Note that key will be
   * extracted from the input row, and the key would be same as the result of getKey(inputRow).
   */
  def put(store: StateStore, row: UnsafeRow): Unit

  /**
   * Commit all the updates that have been made to the target state store, and return the
   * new version.
   */
  def commit(store: StateStore): Long

  /** Remove a single non-null key from the target state store. */
  def remove(store: StateStore, key: UnsafeRow): Unit

  /** Return an iterator containing all the key-value pairs in target state store. */
  def iterator(store: StateStore): Iterator[UnsafeRowPair]

  /** Return an iterator containing all the keys in target state store. */
  def keys(store: StateStore): Iterator[UnsafeRow]

  /** Return an iterator containing all the values in target state store. */
  def values(store: StateStore): Iterator[UnsafeRow]

  /** Check the UnsafeRow format with the expected schema */
  def unsafeRowFormatValidation(row: UnsafeRow, schema: StructType): Unit
}

object StreamingAggregationStateManager extends Logging {
  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  def createStateManager(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      stateFormatVersion: Int): StreamingAggregationStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingAggregationStateManagerImplV1(keyExpressions, inputRowAttributes)
      case 2 => new StreamingAggregationStateManagerImplV2(keyExpressions, inputRowAttributes)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

/**
 * An exception thrown when an invalid UnsafeRow is detected.
 */
class InvalidUnsafeRowException
  extends SparkException("The UnsafeRow format is invalid. This may happen when using the old " +
    "version or broken checkpoint file. To resolve this problem, you can try to restart the " +
    "application or use the legacy way to process streaming state.", null)

abstract class StreamingAggregationStateManagerBaseImpl(
    protected val keyExpressions: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingAggregationStateManager {

  @transient protected lazy val keyProjector =
    GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

  // Consider about the cost, only check the UnsafeRow format for the first row
  private var checkFormat = true

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)

  override def commit(store: StateStore): Long = store.commit()

  override def remove(store: StateStore, key: UnsafeRow): Unit = store.remove(key)

  override def keys(store: StateStore): Iterator[UnsafeRow] = {
    // discard and don't convert values to avoid computation
    store.getRange(None, None).map(_.key)
  }

  override def unsafeRowFormatValidation(row: UnsafeRow, schema: StructType): Unit = {
    if (checkFormat && SQLConf.get.getConf(
        SQLConf.STREAMING_STATE_FORMAT_CHECK_ENABLED) && row != null) {
      if (schema.fields.length != row.numFields) {
        throw new InvalidUnsafeRowException
      }
      schema.fields.zipWithIndex
        .filterNot(field => UnsafeRow.isFixedLength(field._1.dataType)).foreach {
        case (_, index) =>
          val offsetAndSize = row.getLong(index)
          val offset = (offsetAndSize >> 32).toInt
          val size = offsetAndSize.toInt
          if (size < 0 ||
              offset < UnsafeRow.calculateBitSetWidthInBytes(row.numFields) + 8 * row.numFields ||
              offset + size > row.getSizeInBytes) {
            throw new InvalidUnsafeRowException
          }
      }
      checkFormat = false
    }
  }
}

/**
 * The implementation of StreamingAggregationStateManager for state version 1.
 * In state version 1, the schema of key and value in state are follow:
 *
 * - key: Same as key expressions.
 * - value: Same as input row attributes. The schema of value contains key expressions as well.
 *
 * @param keyExpressions The attributes of keys.
 * @param inputRowAttributes The attributes of input row.
 */
class StreamingAggregationStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

  override def getStateValueSchema: StructType = inputRowAttributes.toStructType

  override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
    val res = store.get(key)
    unsafeRowFormatValidation(res, inputRowAttributes.toStructType)
    res
  }

  override def put(store: StateStore, row: UnsafeRow): Unit = {
    store.put(getKey(row), row)
  }

  override def iterator(store: StateStore): Iterator[UnsafeRowPair] = {
    store.iterator()
  }

  override def values(store: StateStore): Iterator[UnsafeRow] = {
    store.iterator().map(_.value)
  }
}

/**
 * The implementation of StreamingAggregationStateManager for state version 2.
 * In state version 2, the schema of key and value in state are follow:
 *
 * - key: Same as key expressions.
 * - value: The diff between input row attributes and key expressions.
 *
 * The schema of value is changed to optimize the memory/space usage in state, via removing
 * duplicated columns in key-value pair. Hence key columns are excluded from the schema of value.
 *
 * @param keyExpressions The attributes of keys.
 * @param inputRowAttributes The attributes of input row.
 */
class StreamingAggregationStateManagerImplV2(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

  private val valueExpressions: Seq[Attribute] = inputRowAttributes.diff(keyExpressions)
  private val keyValueJoinedExpressions: Seq[Attribute] = keyExpressions ++ valueExpressions

  // flag to check whether the row needs to be project into input row attributes after join
  // e.g. if the fields in the joined row are not in the expected order
  private val needToProjectToRestoreValue: Boolean =
    keyValueJoinedExpressions != inputRowAttributes

  @transient private lazy val valueProjector =
    GenerateUnsafeProjection.generate(valueExpressions, inputRowAttributes)

  @transient private lazy val joiner =
    GenerateUnsafeRowJoiner.create(StructType.fromAttributes(keyExpressions),
      StructType.fromAttributes(valueExpressions))
  @transient private lazy val restoreValueProjector = GenerateUnsafeProjection.generate(
    inputRowAttributes, keyValueJoinedExpressions)

  override def getStateValueSchema: StructType = valueExpressions.toStructType

  override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
    val savedState = store.get(key)
    if (savedState == null) {
      return savedState
    }

    val res = restoreOriginalRow(key, savedState)
    unsafeRowFormatValidation(res, inputRowAttributes.toStructType)
    res
  }

  override def put(store: StateStore, row: UnsafeRow): Unit = {
    val key = keyProjector(row)
    val value = valueProjector(row)
    store.put(key, value)
  }

  override def iterator(store: StateStore): Iterator[UnsafeRowPair] = {
    store.iterator().map(rowPair => new UnsafeRowPair(rowPair.key, restoreOriginalRow(rowPair)))
  }

  override def values(store: StateStore): Iterator[UnsafeRow] = {
    store.iterator().map(rowPair => restoreOriginalRow(rowPair))
  }

  private def restoreOriginalRow(rowPair: UnsafeRowPair): UnsafeRow = {
    restoreOriginalRow(rowPair.key, rowPair.value)
  }

  private def restoreOriginalRow(key: UnsafeRow, value: UnsafeRow): UnsafeRow = {
    val joinedRow = joiner.join(key, value)
    if (needToProjectToRestoreValue) {
      restoreValueProjector(joinedRow)
    } else {
      joinedRow
    }
  }
}
