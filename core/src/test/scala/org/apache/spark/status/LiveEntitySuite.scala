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

package org.apache.spark.status

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

class LiveEntitySuite extends SparkFunSuite {

  test("partition seq") {
    val seq = new RDDPartitionSeq()
    val items = (1 to 10).map { i =>
      val part = newPartition(i)
      seq.addPartition(part)
      part
    }.toList

    checkSize(seq, 10)

    val added = newPartition(11)
    seq.addPartition(added)
    checkSize(seq, 11)
    assert(seq.last.blockName === added.blockName)

    seq.removePartition(items(0))
    assert(seq.head.blockName === items(1).blockName)
    assert(!seq.exists(_.blockName == items(0).blockName))
    checkSize(seq, 10)

    seq.removePartition(added)
    assert(seq.last.blockName === items.last.blockName)
    assert(!seq.exists(_.blockName == added.blockName))
    checkSize(seq, 9)

    seq.removePartition(items(5))
    checkSize(seq, 8)
    assert(!seq.exists(_.blockName == items(5).blockName))
  }

  test("rdd tracking in live executor") {
    val exec = new LiveExecutor("1", 0L)
    exec.totalOnHeap = 42L

    assert(!exec.hasRDDData(1))

    exec.addBlock(RDDBlockId(1, 1), 1L, 1L, false)
    assert(exec.hasRDDData(1))
    assert(exec.diskUsed === 1)
    assert(exec.memoryUsed === 1)
    assert(exec.usedOnHeap === 1)
    assert(exec.usedOffHeap === 0)

    exec.addBlock(RDDBlockId(1, 2), 2L, 2L, false)
    assert(exec.hasRDDData(1))
    assert(exec.diskUsed === 3)
    assert(exec.memoryUsed === 3)
    assert(exec.usedOnHeap === 3)
    assert(exec.usedOffHeap === 0)

    val old2 = exec.removeBlock(RDDBlockId(1, 2), false)
    assert(old2 != null)
    assert(old2.diskSize === 2)
    assert(old2.memSize === 2)
    assert(exec.hasRDDData(1))
    assert(exec.diskUsed === 1)
    assert(exec.memoryUsed === 1)
    assert(exec.usedOnHeap === 1)
    assert(exec.usedOffHeap === 0)

    // Update an existing block, as would happen if a block is evicted from memory to disk,
    // or loaded back into memory from disk after space becomes available.
    val old1 = exec.addBlock(RDDBlockId(1, 1), 3L, 3L, false)
    assert(exec.hasRDDData(1))
    assert(old1 != null)
    assert(old1.diskSize === 1)
    assert(old1.memSize === 1)
    assert(exec.diskUsed === 3)
    assert(exec.memoryUsed === 3)
    assert(exec.usedOnHeap === 3)
    assert(exec.usedOffHeap === 0)

    exec.addBlock(RDDBlockId(2, 1), 0L, 4L, true)
    assert(exec.hasRDDData(2))
    assert(exec.diskUsed === 3)
    assert(exec.memoryUsed === 7)
    assert(exec.usedOnHeap === 3)
    assert(exec.usedOffHeap === 4)

    exec.removeBlock(RDDBlockId(1, 1), false)
    assert(exec.cleanupRDD(2, true))
    assert(!exec.hasRDDData(1))
    assert(!exec.hasRDDData(2))
    assert(exec.diskUsed === 0)
    assert(exec.memoryUsed === 0)
  }

  private def checkSize(seq: Seq[_], expected: Int): Unit = {
    assert(seq.length === expected)
    var count = 0
    seq.iterator.foreach { _ => count += 1 }
    assert(count === expected)
  }

  private def newPartition(i: Int): LiveRDDPartition = {
    val part = new LiveRDDPartition(i.toString, StorageLevel.NONE)
    part.update(Seq(i.toString), i, i)
    part
  }

}
