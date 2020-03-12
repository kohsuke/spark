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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.commons.io.FileUtils

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to optimize skewed joins to avoid straggler tasks whose share of data are significantly
 * larger than those of the rest of the tasks.
 *
 * The general idea is to divide each skew partition into smaller partitions and replicate its
 * matching partition on the other side of the join so that they can run in parallel tasks.
 * Note that when matching partitions from the left side and the right side both have skew,
 * it will become a cartesian product of splits from left and right joining together.
 *
 * For example, assume the Sort-Merge join has 4 partitions:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Let's say L2, L4 and R3, R4 are skewed, and each of them get split into 2 sub-partitions. This
 * is scheduled to run 4 tasks at the beginning: (L1, R1), (L2, R2), (L3, R3), (L4, R4).
 * This rule expands it to 9 tasks to increase parallelism:
 * (L1, R1),
 * (L2-1, R2), (L2-2, R2),
 * (L3, R3-1), (L3, R3-2),
 * (L4-1, R4-1), (L4-2, R4-1), (L4-1, R4-2), (L4-2, R4-2)
 *
 * Note that, when this rule is enabled, it also coalesces non-skewed partitions like
 * `CoalesceShufflePartitions` does.
 */
case class OptimizeSkewedJoin(conf: SQLConf) extends Rule[SparkPlan] {

  private val ensureRequirements = EnsureRequirements(conf)

  private val supportedJoinTypes =
    Inner :: Cross :: LeftSemi :: LeftAnti :: LeftOuter :: RightOuter :: Nil

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * ADAPTIVE_EXECUTION_SKEWED_PARTITION_FACTOR and also larger than
   * ADVISORY_PARTITION_SIZE_IN_BYTES.
   */
  private def isSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR) &&
      size > conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
  }

  private def medianSize(stats: MapOutputStatistics): Long = {
    val numPartitions = stats.bytesByPartitionId.length
    val bytes = stats.bytesByPartitionId.sorted
    numPartitions match {
      case _ if (numPartitions % 2 == 0) =>
        math.max((bytes(numPartitions / 2) + bytes(numPartitions / 2 - 1)) / 2, 1)
      case _ => math.max(bytes(numPartitions / 2), 1)
    }
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Seq[Long], medianSize: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filterNot(isSkewed(_, medianSize))
    // It's impossible that all the partitions are skewed, as we use median size to define skew.
    assert(nonSkewSizes.nonEmpty)
    math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
  }

  /**
   * Get the map size of the specific reduce shuffle Id.
   */
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses.map{_.getSizeForBlock(partitionId)}
  }

  /**
   * Split the skewed partition based on the map size and the max split number, and create
   * `PartialReducerPartitionSpec`s for it.
   */
  private def splitAndCreateSpecs(
      shuffleId: Int,
      reducerId: Int,
      targetSize: Long): Option[Seq[ShufflePartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    val mapStartIndices = ShufflePartitionsUtil.splitSizeListByTargetSize(
      mapPartitionSizes, targetSize)
    if (mapStartIndices.length > 1) {
      Some(createSkewPartitions(reducerId, mapStartIndices, mapPartitionSizes.length))
    } else {
      None
    }
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, maxSize: Long): String = {
    s"median size: $medianSize, max size: ${maxSize}"
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin smj.
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle reader that reads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle reader that reads partition0 3 times by
   *    3 tasks separately.
   */
  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleStageInfo), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleStageInfo), _), _)
        if supportedJoinTypes.contains(joinType) =>
      assert(left.partitions.length == right.partitions.length)
      val numPartitions = left.partitions.length
      val leftShuffleId = left.shuffleStage.shuffle.shuffleDependency.shuffleId
      val rightShuffleId = right.shuffleStage.shuffle.shuffleDependency.shuffleId
      // We use the median size of the original shuffle partitions to detect skewed partitions.
      val leftMedSize = medianSize(left.mapStats)
      val rightMedSize = medianSize(right.mapStats)
      logDebug(
        s"""
          |Try to optimize skewed join.
          |Left side partition size:
          |${getSizeInfo(leftMedSize, left.mapStats.bytesByPartitionId.max)}
          |Right side partition size:
          |${getSizeInfo(rightMedSize, right.mapStats.bytesByPartitionId.max)}
        """.stripMargin)
      val canSplitLeft = canSplitLeftSide(joinType)
      val canSplitRight = canSplitRightSide(joinType)
      // We use the actual partition sizes (may be coalesced) to calculate target size, so that
      // the final data distribution is even (coalesced partitions + split partitions).
      val leftSizes = left.partitions.map(_._2)
      val rightSizes = right.partitions.map(_._2)
      val leftTargetSize = targetSize(leftSizes, leftMedSize)
      val rightTargetSize = targetSize(rightSizes, rightMedSize)

      val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val leftSkewDesc = new SkewDesc
      val rightSkewDesc = new SkewDesc
      for (partitionIndex <- 0 until numPartitions) {
        val leftSize = leftSizes(partitionIndex)
        val isLeftSkew = isSkewed(leftSize, leftMedSize) && canSplitLeft
        val rightSize = rightSizes(partitionIndex)
        val isRightSkew = isSkewed(rightSize, rightMedSize) && canSplitRight
        if (isLeftSkew || isRightSkew) {
          val leftParts = if (isLeftSkew) {
            val CoalescedPartitionSpec(start, end) = left.partitions(partitionIndex)._1
            assert(start + 1 == end, "coalesced partition should never be skewed.")
            val specsAfterSplit = splitAndCreateSpecs(leftShuffleId, start, leftTargetSize)
            if (specsAfterSplit.isDefined) {
              leftSkewDesc.addPartitionSize(leftSize)
            }
            specsAfterSplit.getOrElse(Seq(left.partitions(partitionIndex)._1))
          } else {
            Seq(left.partitions(partitionIndex)._1)
          }

          val rightParts = if (isRightSkew) {
            val CoalescedPartitionSpec(start, end) = right.partitions(partitionIndex)._1
            assert(start + 1 == end, "coalesced partition should never be skewed.")
            val specsAfterSplit = splitAndCreateSpecs(rightShuffleId, start, rightTargetSize)
            if (specsAfterSplit.isDefined) {
              rightSkewDesc.addPartitionSize(rightSize)
            }
            specsAfterSplit.getOrElse(Seq(right.partitions(partitionIndex)._1))
          } else {
            Seq(right.partitions(partitionIndex)._1)
          }

          for {
            leftSidePartition <- leftParts
            rightSidePartition <- rightParts
          } {
            leftSidePartitions += leftSidePartition
            rightSidePartitions += rightSidePartition
          }
        } else {
          leftSidePartitions += left.partitions(partitionIndex)._1
          rightSidePartitions += right.partitions(partitionIndex)._1
        }
      }

      logDebug("number of skewed partitions: " +
        s"left ${leftSkewDesc.numPartitions}, right ${rightSkewDesc.numPartitions}")
      if (leftSkewDesc.numPartitions > 0 || rightSkewDesc.numPartitions > 0) {
        val newLeft = CustomShuffleReaderExec(
          left.shuffleStage, leftSidePartitions, leftSkewDesc.toString)
        val newRight = CustomShuffleReaderExec(
          right.shuffleStage, rightSidePartitions, rightSkewDesc.toString)
        smj.copy(
          left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      } else {
        smj
      }
  }

  private def createSkewPartitions(
      reducerIndex: Int,
      mapStartIndices: Seq[Int],
      numMappers: Int): Seq[PartialReducerPartitionSpec] = {
    mapStartIndices.indices.map { i =>
      val startMapIndex = mapStartIndices(i)
      val endMapIndex = if (i == mapStartIndices.length - 1) {
        numMappers
      } else {
        mapStartIndices(i + 1)
      }
      PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)

    if (shuffleStages.length == 2) {
      // When multi table join, there will be too many complex combination to consider.
      // Currently we only handle 2 table join like following two use cases.
      // SMJ
      //   Sort
      //     Shuffle
      //   Sort
      //     Shuffle
      val optimizePlan = optimizeSkewJoin(plan)
      val numShuffles = ensureRequirements.apply(optimizePlan).collect {
        case e: ShuffleExchangeExec => e
      }.length

      if (numShuffles > 0) {
        logDebug("OptimizeSkewedJoin rule is not applied due" +
          " to additional shuffles will be introduced.")
        plan
      } else {
        optimizePlan
      }
    } else {
      plan
    }
  }
}

private object ShuffleStage {
  def unapply(plan: SparkPlan): Option[ShuffleStageInfo] = plan match {
    case s: ShuffleQueryStageExec =>
      val mapStats = getMapStats(s)
      val sizes = mapStats.bytesByPartitionId
      val partitions = sizes.zipWithIndex.map {
        case (size, i) => CoalescedPartitionSpec(i, i + 1) -> size
      }
      Some(ShuffleStageInfo(s, mapStats, partitions))

    case CustomShuffleReaderExec(s: ShuffleQueryStageExec, partitionSpecs, _) =>
      val mapStats = getMapStats(s)
      val sizes = mapStats.bytesByPartitionId
      val partitions = partitionSpecs.map {
        case spec @ CoalescedPartitionSpec(start, end) =>
          var sum = 0L
          var i = start
          while (i < end) {
            sum += sizes(i)
            i += 1
          }
          spec -> sum
        case other => throw new IllegalArgumentException(
          s"Expect CoalescedPartitionSpec but got $other")
      }
      Some(ShuffleStageInfo(s, mapStats, partitions))

    case _ => None
  }

  private def getMapStats(stage: ShuffleQueryStageExec): MapOutputStatistics = {
    assert(stage.resultOption.isDefined, "ShuffleQueryStageExec should" +
      " already be ready when executing OptimizeSkewedPartitions rule")
    stage.resultOption.get.asInstanceOf[MapOutputStatistics]
  }
}

private case class ShuffleStageInfo(
    shuffleStage: ShuffleQueryStageExec,
    mapStats: MapOutputStatistics,
    partitions: Seq[(CoalescedPartitionSpec, Long)])

private class SkewDesc {
  private[this] var numSkewedPartitions: Int = 0
  private[this] var totalSize: Long = 0
  private[this] var maxSize: Long = 0
  private[this] var minSize: Long = 0

  def numPartitions: Int = numSkewedPartitions

  def addPartitionSize(size: Long): Unit = {
    if (numSkewedPartitions == 0) {
      maxSize = size
      minSize = size
    }
    numSkewedPartitions += 1
    totalSize += size
    if (size > maxSize) maxSize = size
    if (size < minSize) minSize = size
  }

  override def toString: String = {
    if (numSkewedPartitions == 0) {
      "no skewed partition"
    } else {
      val maxSizeStr = FileUtils.byteCountToDisplaySize(maxSize)
      val minSizeStr = FileUtils.byteCountToDisplaySize(minSize)
      val avgSizeStr = FileUtils.byteCountToDisplaySize(totalSize / numSkewedPartitions)
      s"$numSkewedPartitions skewed partitions with " +
        s"size(max=$maxSizeStr, min=$minSizeStr, avg=$avgSizeStr)"
    }
  }
}
