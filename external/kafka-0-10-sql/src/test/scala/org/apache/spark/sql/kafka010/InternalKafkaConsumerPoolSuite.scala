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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.SparkEnv
import org.apache.spark.sql.kafka010.KafkaDataConsumer.CacheKey
import org.apache.spark.sql.test.SharedSparkSession

class InternalKafkaConsumerPoolSuite extends SharedSparkSession {

  test("basic multiple borrows and returns for single key") {
    val pool = InternalKafkaConsumerPool.build

    val topic = "topic"
    val partitionId = 0
    val topicPartition = new TopicPartition(topic, partitionId)

    val kafkaParams: ju.Map[String, Object] = getTestKafkaParams

    val key = new CacheKey(topicPartition, kafkaParams)

    val pooledObjects = (0 to 2).map { _ =>
      val pooledObject = pool.borrowObject(key, kafkaParams)
      assertPooledObject(pooledObject, topicPartition, kafkaParams)
      pooledObject
    }

    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 3, numTotal = 3)
    assertPoolState(pool, numIdle = 0, numActive = 3, numTotal = 3)

    val pooledObject2 = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject2, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 4, numTotal = 4)
    assertPoolState(pool, numIdle = 0, numActive = 4, numTotal = 4)

    pooledObjects.foreach(pool.returnObject)

    assertPoolStateForKey(pool, key, numIdle = 3, numActive = 1, numTotal = 4)
    assertPoolState(pool, numIdle = 3, numActive = 1, numTotal = 4)

    pool.returnObject(pooledObject2)

    // we only allow three idle objects per key
    assertPoolStateForKey(pool, key, numIdle = 3, numActive = 0, numTotal = 3)
    assertPoolState(pool, numIdle = 3, numActive = 0, numTotal = 3)

    pool.close()
  }

  test("basic borrow and return for multiple keys") {
    val pool = InternalKafkaConsumerPool.build

    val kafkaParams = getTestKafkaParams
    val topicPartitions = createTopicPartitions(Seq("topic", "topic2"), 6)
    val keys = createCacheKeys(topicPartitions, kafkaParams)

    // while in loop pool doesn't still exceed total pool size
    val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

    assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length,
      numTotal = keyToPooledObjectPairs.length)

    returnObjects(pool, keyToPooledObjectPairs)

    assertPoolState(pool, numIdle = keyToPooledObjectPairs.length, numActive = 0,
      numTotal = keyToPooledObjectPairs.length)

    pool.close()
  }

  test("borrow more than soft max capacity from pool which is neither free space nor idle object") {
    val capacity = 16
    val newConf = newConfForKafkaPool(Some(capacity), Some(-1), Some(-1))

    withSparkConf(newConf: _*) {
      val pool = InternalKafkaConsumerPool.build

      val kafkaParams = getTestKafkaParams
      val topicPartitions = createTopicPartitions(Seq("topic"), capacity)
      val keys = createCacheKeys(topicPartitions, kafkaParams)

      // while in loop pool doesn't still exceed soft max pool size
      val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

      val moreTopicPartition = new TopicPartition("topic2", 0)
      val newCacheKey = new CacheKey(moreTopicPartition, kafkaParams)

      // exceeds soft max pool size, and also no idle object for cleaning up
      // but pool will borrow a new object
      pool.borrowObject(newCacheKey, kafkaParams)

      assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length + 1,
        numTotal = keyToPooledObjectPairs.length + 1)

      pool.close()
    }
  }

  test("borrow more than soft max capacity from pool frees up idle objects automatically") {
    val capacity = 16
    val newConf = newConfForKafkaPool(Some(capacity), Some(-1), Some(-1))

    withSparkConf(newConf: _*) {
      val pool = InternalKafkaConsumerPool.build

      val kafkaParams = getTestKafkaParams
      val topicPartitions = createTopicPartitions(Seq("topic"), capacity)
      val keys = createCacheKeys(topicPartitions, kafkaParams)

      // borrow objects which makes pool reaching soft capacity
      val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

      // return 20% of objects to ensure there're some idle objects to free up later
      val numToReturn = (keyToPooledObjectPairs.length * 0.2).toInt
      returnObjects(pool, keyToPooledObjectPairs.take(numToReturn))

      assertPoolState(pool, numIdle = numToReturn,
        numActive = keyToPooledObjectPairs.length - numToReturn,
        numTotal = keyToPooledObjectPairs.length)

      // borrow a new object: there should be some idle objects to clean up
      val moreTopicPartition = new TopicPartition("topic2", 0)
      val newCacheKey = new CacheKey(moreTopicPartition, kafkaParams)

      val newObject = pool.borrowObject(newCacheKey, kafkaParams)
      assertPooledObject(newObject, moreTopicPartition, kafkaParams)
      assertPoolStateForKey(pool, newCacheKey, numIdle = 0, numActive = 1, numTotal = 1)

      // at least one of idle object should be freed up
      assert(pool.getNumIdle < numToReturn)
      // we can determine number of active objects correctly
      assert(pool.getNumActive === keyToPooledObjectPairs.length - numToReturn + 1)
      // total objects should be more than number of active + 1 but can't expect exact number
      assert(pool.getTotal > keyToPooledObjectPairs.length - numToReturn + 1)

      pool.close()
    }
  }

  test("evicting idle objects on background") {
    import org.scalatest.time.SpanSugar._

    val minEvictableIdleTimeMillis = 3 * 1000 // 3 seconds
    val evictorThreadRunIntervalMillis = 500 // triggering multiple evictions by intention

    val newConf = newConfForKafkaPool(None, Some(minEvictableIdleTimeMillis),
      Some(evictorThreadRunIntervalMillis))
    withSparkConf(newConf: _*) {
      val pool = InternalKafkaConsumerPool.build

      val kafkaParams = getTestKafkaParams
      val topicPartitions = createTopicPartitions(Seq("topic"), 10)
      val keys = createCacheKeys(topicPartitions, kafkaParams)

      // borrow and return some consumers to ensure some partitions are being idle
      // this test covers the use cases: rebalance / topic removal happens while running query
      val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)
      val objectsToReturn = keyToPooledObjectPairs.filter(_._1.topicPartition.partition() % 2 == 0)
      returnObjects(pool, objectsToReturn)

      // wait up to twice than minEvictableIdleTimeMillis to ensure evictor thread to clear up
      // idle objects
      eventually(timeout((minEvictableIdleTimeMillis.toLong * 2).seconds),
        interval(evictorThreadRunIntervalMillis.milliseconds)) {
        assertPoolState(pool, numIdle = 0, numActive = 5, numTotal = 5)
      }

      pool.close()
    }
  }

  private def newConfForKafkaPool(
      capacity: Option[Int],
      minEvictableIdleTimeMillis: Option[Long],
      evictorThreadRunIntervalMillis: Option[Long]): Seq[(String, String)] = {
    Seq(
      CONSUMER_CACHE_CAPACITY.key -> capacity,
      CONSUMER_CACHE_MIN_EVICTABLE_IDLE_TIME_MILLIS.key -> minEvictableIdleTimeMillis,
      CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL_MILLIS.key -> evictorThreadRunIntervalMillis
    ).filter(_._2.isDefined).map(e => (e._1 -> e._2.get.toString))
  }

  private def createTopicPartitions(
      topicNames: Seq[String],
      countPartition: Int): List[TopicPartition] = {
    for (
      topic <- topicNames.toList;
      partitionId <- 0 until countPartition
    ) yield new TopicPartition(topic, partitionId)
  }

  private def createCacheKeys(
      topicPartitions: List[TopicPartition],
      kafkaParams: ju.Map[String, Object]): List[CacheKey] = {
    topicPartitions.map(new CacheKey(_, kafkaParams))
  }

  private def assertPooledObject(
      pooledObject: InternalKafkaConsumer,
      expectedTopicPartition: TopicPartition,
      expectedKafkaParams: ju.Map[String, Object]): Unit = {
    assert(pooledObject != null)
    assert(pooledObject.kafkaParams === expectedKafkaParams)
    assert(pooledObject.topicPartition === expectedTopicPartition)
  }

  private def assertPoolState(
      pool: InternalKafkaConsumerPool,
      numIdle: Int,
      numActive: Int,
      numTotal: Int): Unit = {
    assert(pool.getNumIdle === numIdle)
    assert(pool.getNumActive === numActive)
    assert(pool.getTotal === numTotal)
  }

  private def assertPoolStateForKey(
      pool: InternalKafkaConsumerPool,
      key: CacheKey,
      numIdle: Int,
      numActive: Int,
      numTotal: Int): Unit = {
    assert(pool.getNumIdle(key) === numIdle)
    assert(pool.getNumActive(key) === numActive)
    assert(pool.getTotal(key) === numTotal)
  }

  private def getTestKafkaParams: ju.Map[String, Object] = Map[String, Object](
    GROUP_ID_CONFIG -> "groupId",
    BOOTSTRAP_SERVERS_CONFIG -> "PLAINTEXT://localhost:9092",
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  private def borrowObjectsPerKey(
      pool: InternalKafkaConsumerPool,
      kafkaParams: ju.Map[String, Object],
      keys: List[CacheKey]): Seq[(CacheKey, InternalKafkaConsumer)] = {
    keys.map { key =>
      val numActiveBeforeBorrowing = pool.getNumActive
      val numIdleBeforeBorrowing = pool.getNumIdle
      val numTotalBeforeBorrowing = pool.getTotal

      val pooledObj = pool.borrowObject(key, kafkaParams)

      assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeBorrowing,
        numActive = numActiveBeforeBorrowing + 1, numTotal = numTotalBeforeBorrowing + 1)

      (key, pooledObj)
    }
  }

  private def returnObjects(
      pool: InternalKafkaConsumerPool,
      objects: Seq[(CacheKey, InternalKafkaConsumer)]): Unit = {
    objects.foreach { case (key, pooledObj) =>
      val numActiveBeforeReturning = pool.getNumActive
      val numIdleBeforeReturning = pool.getNumIdle
      val numTotalBeforeReturning = pool.getTotal

      pool.returnObject(pooledObj)

      // we only allow one idle object per key
      assertPoolStateForKey(pool, key, numIdle = 1, numActive = 0, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeReturning + 1,
        numActive = numActiveBeforeReturning - 1, numTotal = numTotalBeforeReturning)
    }
  }

  private def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf

    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.get(key))
      } else {
        None
      }
    }

    (keys, values).zipped.foreach { conf.set }

    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.remove(key)
      }
    }
  }
}
