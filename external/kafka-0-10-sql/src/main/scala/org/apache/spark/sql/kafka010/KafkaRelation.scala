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

package org.apache.spark.sql.kafka010

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


private[kafka010] class KafkaRelation(
    override val sqlContext: SQLContext,
    strategy: ConsumerStrategy,
    sourceOptions: CaseInsensitiveMap[String],
    specifiedKafkaParams: Map[String, String],
    failOnDataLoss: Boolean,
    startingOffsets: KafkaOffsetRangeLimit,
    endingOffsets: KafkaOffsetRangeLimit)
  extends BaseRelation with TableScan with Logging {
  assert(startingOffsets != LatestOffsetRangeLimit,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsetRangeLimit,
    "Ending offset not allowed to be set to earliest offsets.")

  private val pollTimeoutMs = sourceOptions.getOrElse(
    KafkaSourceProvider.CONSUMER_POLL_TIMEOUT,
    (sqlContext.sparkContext.conf.get(NETWORK_TIMEOUT) * 1000L).toString
  ).toLong

  override def schema: StructType = KafkaOffsetReader.kafkaSchema

  override def buildScan(): RDD[Row] = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = KafkaSourceProvider.batchUniqueGroupId(sourceOptions)

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy,
      KafkaSourceProvider.kafkaParamsForDriver(specifiedKafkaParams),
      sourceOptions,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    // Leverage the KafkaReader to obtain the relevant partition offsets
    val offsetRanges: Seq[KafkaOffsetRange] = try {
      val fromPartitionOffsets = kafkaOffsetReader.fetchPartitionOffsets(startingOffsets)
      val untilPartitionOffsets = kafkaOffsetReader.fetchPartitionOffsets(endingOffsets)
      kafkaOffsetReader.getOffsetRanges(fromPartitionOffsets, untilPartitionOffsets)
    } finally {
      kafkaOffsetReader.close()
    }

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val executorKafkaParams =
      KafkaSourceProvider.kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId)
    val rdd = new KafkaSourceRDD(
      sqlContext.sparkContext, executorKafkaParams, offsetRanges,
      pollTimeoutMs, failOnDataLoss, reuseKafkaConsumer = false).map { cr =>
      InternalRow(
        cr.key,
        cr.value,
        UTF8String.fromString(cr.topic),
        cr.partition,
        cr.offset,
        DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(cr.timestamp)),
        cr.timestampType.id)
    }
    sqlContext.internalCreateDataFrame(rdd.setName("kafka"), schema).rdd
  }

  override def toString: String =
    s"KafkaRelation(strategy=$strategy, start=$startingOffsets, end=$endingOffsets)"
}
