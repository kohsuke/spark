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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.{sameOutput, JoinPlan}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf


/**
 * Cost-based join reorder.
 * We may have several join reorder algorithms in the future. This class is the entry of these
 * algorithms, and chooses which one to use.
 */
object CostBasedJoinReorder extends Rule[LogicalPlan] with PredicateHelper {

  private def conf = SQLConf.get

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.cboEnabled || !conf.joinReorderEnabled) {
      plan
    } else {
      val result = plan transformDown {
        // Start reordering with a joinable item, which is an InnerLike join with conditions.
        // Avoid reordering if a join hint is present.
        case j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE) =>
          reorder(j, j.output)
        case p @ Project(projectList, Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE))
          if projectList.forall(_.isInstanceOf[Attribute]) =>
          reorder(p, p.output)
      }
      // After reordering is finished, convert OrderedJoin back to Join.
      result transform {
        case OrderedJoin(left, right, jt, cond) => Join(left, right, jt, cond, JoinHint.NONE)
      }
    }
  }

  private def reorder(plan: LogicalPlan, output: Seq[Attribute]): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    // Do reordering if the join conditions exist.
    // We also need to check if costs of all items can be evaluated.
    val canPerformReorder = items.size > 2 && conditions.nonEmpty &&
        items.forall(_.stats.rowCount.isDefined)
    val result =
      // Reorder with DP when the the number of items is appropriate,
      // otherwise try GA if it is enabled.
      if (canPerformReorder && items.size <= conf.joinReorderDPThreshold) {
        JoinReorderDP.search(conf, items, conditions, output)
      } else if (canPerformReorder && conf.joinReorderGAEnabled) {
        JoinReorderGA.search(conf, items, conditions, output).getOrElse(plan)
      } else {
        plan
      }
    // Set consecutive join nodes ordered.
    replaceWithOrderedJoin(result)
  }

  /**
   * Extracts items of consecutive inner joins and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Set[Expression]) = {
    plan match {
      case Join(left, right, _: InnerLike, Some(cond), JoinHint.NONE) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, splitConjunctivePredicates(cond).toSet ++
          leftConditions ++ rightConditions)
      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), Set())
    }
  }

  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
    case j @ Join(left, right, jt: InnerLike, Some(cond), JoinHint.NONE) =>
      val replacedLeft = replaceWithOrderedJoin(left)
      val replacedRight = replaceWithOrderedJoin(right)
      OrderedJoin(replacedLeft, replacedRight, jt, Some(cond))
    case p @ Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond), JoinHint.NONE)) =>
      p.copy(child = replaceWithOrderedJoin(j))
    case _ =>
      plan
  }
}

/** This is a mimic class for a join node that has been ordered. */
case class OrderedJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}

/**
 * Reorder the joins using a dynamic programming algorithm. This implementation is based on the
 * paper: Access Path Selection in a Relational Database Management System.
 * http://www.inf.ed.ac.uk/teaching/courses/adbs/AccessPath.pdf
 *
 * First we put all items (basic joined nodes) into level 0, then we build all two-way joins
 * at level 1 from plans at level 0 (single items), then build all 3-way joins from plans
 * at previous levels (two-way joins and single items), then 4-way joins ... etc, until we
 * build all n-way joins and pick the best plan among them.
 *
 * When building m-way joins, we only keep the best plan (with the lowest cost) for the same set
 * of m items. E.g., for 3-way joins, we keep only the best plan for items {A, B, C} among
 * plans (A J B) J C, (A J C) J B and (B J C) J A.
 * We also prune cartesian product candidates when building a new plan if there exists no join
 * condition involving references from both left and right. This pruning strategy significantly
 * reduces the search space.
 * E.g., given A J B J C J D with join conditions A.k1 = B.k1 and B.k2 = C.k2 and C.k3 = D.k3,
 * plans maintained for each level are as follows:
 * level 0: p({A}), p({B}), p({C}), p({D})
 * level 1: p({A, B}), p({B, C}), p({C, D})
 * level 2: p({A, B, C}), p({B, C, D})
 * level 3: p({A, B, C, D})
 * where p({A, B, C, D}) is the final output plan.
 *
 * For cost evaluation, since physical costs for operators are not available currently, we use
 * cardinalities and sizes to compute costs.
 */
object JoinReorderDP extends PredicateHelper with Logging {

  def search(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      output: Seq[Attribute]): LogicalPlan = {

    val startTime = System.nanoTime()
    // Level i maintains all found plans for i + 1 items.
    // Create the initial plans: each plan is a single item with zero cost.
    val itemIndex = items.zipWithIndex
    val foundPlans = mutable.Buffer[JoinPlanMap](itemIndex.map {
      case (item, id) => Set(id) -> JoinPlan(Set(id), item, Set.empty, Cost(0, 0))
    }.toMap)

    // Build filters from the join graph to be used by the search algorithm.
    val filters = JoinReorderDPFilters.buildJoinGraphInfo(conf, items, conditions, itemIndex)

    // Build plans for next levels until the last level has only one plan. This plan contains
    // all items that can be joined, so there's no need to continue.
    val topOutputSet = AttributeSet(output)
    while (foundPlans.size < items.length) {
      // Build plans for the next level.
      foundPlans += searchLevel(foundPlans, conf, conditions, topOutputSet, filters)
    }

    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logDebug(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
      s"${items.length}, number of plans in memo: ${foundPlans.map(_.size).sum}")

    // The last level must have one and only one plan, because all items are joinable.
    assert(foundPlans.size == items.length && foundPlans.last.size == 1)
    foundPlans.last.head._2.plan match {
      case p @ Project(projectList, j: Join) if projectList != output =>
        assert(topOutputSet == p.outputSet)
        // Keep the same order of final output attributes.
        p.copy(projectList = output)
      case finalPlan if !sameOutput(finalPlan, output) =>
        Project(output, finalPlan)
      case finalPlan =>
        finalPlan
    }
  }

  def sameOutput(plan: LogicalPlan, expectedOutput: Seq[Attribute]): Boolean = {
    val thisOutput = plan.output
    thisOutput.length == expectedOutput.length && thisOutput.zip(expectedOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  /** Find all possible plans at the next level, based on existing levels. */
  private def searchLevel(
      existingLevels: Seq[JoinPlanMap],
      conf: SQLConf,
      conditions: Set[Expression],
      topOutput: AttributeSet,
      filters: Option[JoinGraphInfo]): JoinPlanMap = {

    val nextLevel = mutable.Map.empty[Set[Int], JoinPlan]
    var k = 0
    val lev = existingLevels.length - 1
    // Build plans for the next level from plans at level k (one side of the join) and level
    // lev - k (the other side of the join).
    // For the lower level k, we only need to search from 0 to lev - k, because when building
    // a join from A and B, both A J B and B J A are handled.
    while (k <= lev - k) {
      val oneSideCandidates = existingLevels(k).values.toSeq
      for (i <- oneSideCandidates.indices) {
        val oneSidePlan = oneSideCandidates(i)
        val otherSideCandidates = if (k == lev - k) {
          // Both sides of a join are at the same level, no need to repeat for previous ones.
          oneSideCandidates.drop(i)
        } else {
          existingLevels(lev - k).values.toSeq
        }

        otherSideCandidates.foreach { otherSidePlan =>
          JoinReorderUtils
              .buildJoin(oneSidePlan, otherSidePlan, conf, conditions, topOutput, filters) match {
            case Some(newJoinPlan) =>
              // Check if it's the first plan for the item set, or it's a better plan than
              // the existing one due to lower cost.
              val existingPlan = nextLevel.get(newJoinPlan.itemIds)
              if (existingPlan.isEmpty || newJoinPlan.betterThan(existingPlan.get, conf)) {
                nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
              }
            case None =>
          }
        }
      }
      k += 1
    }
    nextLevel.toMap
  }

  /** Map[set of item ids, join plan for these items] */
  type JoinPlanMap = Map[Set[Int], JoinPlan]

  /**
   * Partial join order in a specific level.
   *
   * @param itemIds Set of item ids participating in this partial plan.
   * @param plan The plan tree with the lowest cost for these items found so far.
   * @param joinConds Join conditions included in the plan.
   * @param planCost The cost of this plan tree is the sum of costs of all intermediate joins.
   */
  case class JoinPlan(
      itemIds: Set[Int],
      plan: LogicalPlan,
      joinConds: Set[Expression],
      planCost: Cost) {

    /** Get the cost of the root node of this plan tree. */
    def rootCost(conf: SQLConf): Cost = {
      if (itemIds.size > 1) {
        val rootStats = plan.stats
        Cost(rootStats.rowCount.get, rootStats.sizeInBytes)
      } else {
        // If the plan is a leaf item, it has zero cost.
        Cost(0, 0)
      }
    }

    def betterThan(other: JoinPlan, conf: SQLConf): Boolean = {
      if (other.planCost.card == 0 || other.planCost.size == 0) {
        false
      } else {
        val relativeRows = BigDecimal(this.planCost.card) / BigDecimal(other.planCost.card)
        val relativeSize = BigDecimal(this.planCost.size) / BigDecimal(other.planCost.size)
        relativeRows * conf.joinReorderCardWeight +
          relativeSize * (1 - conf.joinReorderCardWeight) < 1
      }
    }
  }
}

/**
 * This class defines the cost model for a plan.
 * @param card Cardinality (number of rows).
 * @param size Size in bytes.
 */
case class Cost(card: BigInt, size: BigInt) {
  def +(other: Cost): Cost = Cost(this.card + other.card, this.size + other.size)
}

/**
 * Implements optional filters to reduce the search space for join enumeration.
 *
 * 1) Star-join filters: Plan star-joins together since they are assumed
 *    to have an optimal execution based on their RI relationship.
 * 2) Cartesian products: Defer their planning later in the graph to avoid
 *    large intermediate results (expanding joins, in general).
 * 3) Composite inners: Don't generate "bushy tree" plans to avoid materializing
 *   intermediate results.
 *
 * Filters (2) and (3) are not implemented.
 */
object JoinReorderDPFilters extends PredicateHelper {
  /**
   * Builds join graph information to be used by the filtering strategies.
   * Currently, it builds the sets of star/non-star joins.
   * It can be extended with the sets of connected/unconnected joins, which
   * can be used to filter Cartesian products.
   */
  def buildJoinGraphInfo(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      itemIndex: Seq[(LogicalPlan, Int)]): Option[JoinGraphInfo] = {

    if (conf.joinReorderDPStarFilter) {
      // Compute the tables in a star-schema relationship.
      val starJoin = StarSchemaDetection.findStarJoins(items, conditions.toSeq)
      val nonStarJoin = items.filterNot(starJoin.contains(_))

      if (starJoin.nonEmpty && nonStarJoin.nonEmpty) {
        val itemMap = itemIndex.toMap
        Some(JoinGraphInfo(starJoin.map(itemMap).toSet, nonStarJoin.map(itemMap).toSet))
      } else {
        // Nothing interesting to return.
        None
      }
    } else {
      // Star schema filter is not enabled.
      None
    }
  }

  /**
   * Applies the star-join filter that eliminates join combinations among star
   * and non-star tables until the star join is built.
   *
   * Given the oneSideJoinPlan/otherSideJoinPlan, which represent all the plan
   * permutations generated by the DP join enumeration, and the star/non-star plans,
   * the following plan combinations are allowed:
   * 1. (oneSideJoinPlan U otherSideJoinPlan) is a subset of star-join
   * 2. star-join is a subset of (oneSideJoinPlan U otherSideJoinPlan)
   * 3. (oneSideJoinPlan U otherSideJoinPlan) is a subset of non star-join
   *
   * It assumes the sets are disjoint.
   *
   * Example query graph:
   *
   * t1   d1 - t2 - t3
   *  \  /
   *   f1
   *   |
   *   d2
   *
   * star: {d1, f1, d2}
   * non-star: {t2, t1, t3}
   *
   * level 0: (f1 ), (d2 ), (t3 ), (d1 ), (t1 ), (t2 )
   * level 1: {t3 t2 }, {f1 d2 }, {f1 d1 }
   * level 2: {d2 f1 d1 }
   * level 3: {t1 d1 f1 d2 }, {t2 d1 f1 d2 }
   * level 4: {d1 t2 f1 t1 d2 }, {d1 t3 t2 f1 d2 }
   * level 5: {d1 t3 t2 f1 t1 d2 }
   *
   * @param oneSideJoinPlan One side of the join represented as a set of plan ids.
   * @param otherSideJoinPlan The other side of the join represented as a set of plan ids.
   * @param filters Star and non-star plans represented as sets of plan ids
   */
  def starJoinFilter(
      oneSideJoinPlan: Set[Int],
      otherSideJoinPlan: Set[Int],
      filters: JoinGraphInfo) : Boolean = {
    val starJoins = filters.starJoins
    val nonStarJoins = filters.nonStarJoins
    val join = oneSideJoinPlan.union(otherSideJoinPlan)

    // Disjoint sets
    oneSideJoinPlan.intersect(otherSideJoinPlan).isEmpty &&
      // Either star or non-star is empty
      (starJoins.isEmpty || nonStarJoins.isEmpty ||
        // Join is a subset of the star-join
        join.subsetOf(starJoins) ||
        // Star-join is a subset of join
        starJoins.subsetOf(join) ||
        // Join is a subset of non-star
        join.subsetOf(nonStarJoins))
  }
}

/**
 * Helper class that keeps information about the join graph as sets of item/plan ids.
 * It currently stores the star/non-star plans. It can be
 * extended with the set of connected/unconnected plans.
 */
case class JoinGraphInfo (starJoins: Set[Int], nonStarJoins: Set[Int])

/**
 * Reorder the joins using a genetic algorithm. The algorithm treat the reorder problem
 * to a traveling salesmen problem, and use genetic algorithm give an optimized solution.
 *
 * The implementation refs the geqo in postgresql, which is contibuted by Darrell Whitley:
 * https://www.postgresql.org/docs/9.1/geqo-pg-intro.html
 *
 * For more info about genetic algorithm and the edge recombination crossover, pls see:
 * "A Genetic Algorithm Tutorial, Darrell Whitley"
 * https://link.springer.com/article/10.1007/BF00175354
 * and "Scheduling Problems and Traveling Salesmen: The Genetic Edge Recombination Operator,
 * Darrell Whitley et al." https://dl.acm.org/citation.cfm?id=657238
 * respectively.
 */
object JoinReorderGA extends PredicateHelper with Logging {

  def search(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      output: Seq[Attribute]): Option[LogicalPlan] = {

    val startTime = System.nanoTime()

    val itemsWithIndex = items.zipWithIndex.map {
      case (plan, id) => id -> JoinPlan(Set(id), plan, Set.empty, Cost(0, 0))
    }.toMap

    val topOutputSet = AttributeSet(output)

    val pop = Population(conf, itemsWithIndex, conditions, topOutputSet).evolve

    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logInfo(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
        s"${items.length}, number of plans in memo: ${ pop.chromos.size}")

    assert(pop.chromos.head.basicPlans.size == items.length)
    pop.chromos.head.integratedPlan match {
      case Some(joinPlan) => joinPlan.plan match {
        case p @ Project(projectList, _: Join) if projectList != output =>
          assert(topOutputSet == p.outputSet)
          // Keep the same order of final output attributes.
          Some(p.copy(projectList = output))
        case finalPlan if !sameOutput(finalPlan, output) =>
          Some(Project(output, finalPlan))
        case finalPlan =>
          Some(finalPlan)
      }
      case _ => None
    }
  }
}

/**
 * A pair of parent individuals can breed a child with certain crossover process.
 * With crossover, child can inherit gene from its parents, and these gene snippets
 * finally compose a new [[Chromosome]].
 */
@DeveloperApi
trait Crossover {

  /**
   * Generate a new [[Chromosome]] from the given parent [[Chromosome]]s,
   * with this crossover algorithm.
   */
  def newChromo(father: Chromosome, mother: Chromosome) : Chromosome
}

/**
 * This class implements the Genetic Edge Recombination algorithm. The algorithm generates
 * a new traveling path by choosing the edges in certain order from the parents' edge table,
 * where the edge table contains links of vertexes of the parents' traveling paths.
 *
 * Here's an example. Suppose we have two traveling paths on a graph,
 *      I, [A B C D E F]
 *     II, [B D C A E F]
 *
 * The algorithm works as follows,
 *  1. find the links of each vertex, then we have an 'Edge Table'
 *      A: B F C E
 *      B: A C D F
 *      C: B D A
 *      D: C E B
 *      E: D F A
 *      F: A E B
 *  2. from one vertex, say A, choose one of it's neighbours as it's new neighbour, say F, we have
 *      {A F}
 *  3. then choose one from F's neighbours(note that vertex that has been chosen should not
 *     be chosen again),
 *      {A F E}
 *  4. go on,
 *      {A F E D}
 *  5. go on,
 *      {A F E D B}
 *  6. go on,
 *      {A F E D B C}
 *  Note if the procedure ends before all of vertexes been chosen, start from another vertex that
 *  has not been chosen, and go on the procedure.
 *
 * For more information about the Genetic Edge Recombination,
 * see "Scheduling Problems and Traveling Salesmen: The Genetic Edge
 * Recombination Operator" by Darrell Whitley et al.
 * https://dl.acm.org/citation.cfm?id=657238
 */
object EdgeRecombination extends Crossover {

  def genEdgeTable(father: Chromosome, mother: Chromosome) : Map[JoinPlan, Seq[JoinPlan]] = {
    val fatherTable = father.basicPlans.map(g => g -> findNeighbours(father.basicPlans, g)).toMap
    val motherTable = mother.basicPlans.map(g => g -> findNeighbours(mother.basicPlans, g)).toMap

    fatherTable.map(entry => entry._1 -> (entry._2 ++ motherTable(entry._1)))
  }

  def findNeighbours(genes: Seq[JoinPlan], g: JoinPlan) : Seq[JoinPlan] = {
    val genesIndexed = genes.toIndexedSeq
    val index = genesIndexed.indexOf(g)
    val length = genes.size
    if (index > 0 && index < length - 1) {
      Seq(genesIndexed(index - 1), genesIndexed(index + 1))
    } else if (index == 0) {
      Seq(genesIndexed(1), genesIndexed(length - 1))
    } else if (index == length - 1) {
      Seq(genesIndexed(0), genesIndexed(length - 2))
    } else {
      Seq()
    }
  }

  override def newChromo(father: Chromosome, mother: Chromosome): Chromosome = {
    var newGenes: Seq[JoinPlan] = Seq()
    // 1. Generate the edge table.
    var table = genEdgeTable(father, mother)
    // 2. Choose a start vertex randomly from the heads of father/mother.
    var current =
      if (util.Random.nextInt(2) == 0) father.basicPlans.head else mother.basicPlans.head
    newGenes :+= current

    var stop = false
    while (!stop) {
      // 3. Filter out the chosen vertex from the edge table.
      table = table.map(
        entry => entry._1 -> entry._2.filter(_ != current)
      )
      // 4. Choose next vertex among its neighbours. The criterion for choosing which point
      // is that the one who has fewest neighbours. If two or more points has the same num
      // of neighbours, choose one randomly. If there's no neighbours available for this
      // point but there're still other remaining vertexes, choose one from them randomly.
      val tobeVisited = table(current)
      val neighboursTable = tobeVisited.map(g => g -> table(g)).sortBy(-_._2.size).toMap
      val filteredTable = table.filter(_._2.nonEmpty)
      if (neighboursTable.nonEmpty) {
        val numBase = neighboursTable.head._2.size
        var numCand = 0
        neighboursTable.foreach(entry => if (entry._2.size == numBase) numCand += 1)
        current = neighboursTable.toIndexedSeq(util.Random.nextInt(numCand))._1
        newGenes :+= current
      } else if (filteredTable.nonEmpty) {
        current = filteredTable.toIndexedSeq(util.Random.nextInt(filteredTable.size))._2.head
        newGenes :+= current
      } else {
        stop = true
      }
    }

    Chromosome(father.conf, newGenes, father.conditions, father.topOutputSet)
  }
}

/**
 * A sequence of genes(each represents a single relation) which represents
 * a joined plan with determined join order.
 */
case class Chromosome(
    conf: SQLConf,
    basicPlans: Seq[JoinPlan],
    conditions: Set[Expression],
    topOutputSet: AttributeSet) {

  lazy val fitness: Double = evalFitness(integratedPlan)

  lazy val integratedPlan: Option[JoinPlan] = makePlan

  private def makePlan: Option[JoinPlan] = {
    val semiFinished = mutable.Buffer[JoinPlan]()
    basicPlans.foreach(mergeSemi(semiFinished, _))
    if (semiFinished.head.itemIds.size == basicPlans.size) {
      Some(semiFinished.head)
    } else {
      None
    }
  }

  private def mergeSemi(semiFinished: mutable.Buffer[JoinPlan], right: JoinPlan): Unit = {
    val filters = None: Option[JoinGraphInfo]
    for (left <- semiFinished) {
      JoinReorderUtils.buildJoin(left, right, conf, conditions, topOutputSet, filters) match {
        case Some(joined) =>
          semiFinished.remove(semiFinished.indexOf(left))
          mergeSemi(semiFinished, joined)
        case _ =>
          None
      }
    }

    if (semiFinished.isEmpty || right.itemIds.size == 1) {
      semiFinished.append(right)
      return
    }

    insertPlan(semiFinished, right)
  }

  private def insertPlan(semiFinished: mutable.Buffer[JoinPlan], plan: JoinPlan): Unit = {
    var criticalSize = if (semiFinished.head.itemIds.size > plan.itemIds.size) {
      semiFinished.head.itemIds.size
    } else {
      plan.itemIds.size
    }
    var criticalIndex = 0
    var break: Boolean = false
    for (p <- semiFinished if !break) {
      if (plan.itemIds.size > p.itemIds.size && plan.itemIds.size <= criticalSize) {
        break = true
      } else {
        criticalIndex += 1
        criticalSize = p.itemIds.size
      }
    }

    semiFinished.insert(criticalIndex, plan)
  }

  private def evalFitness(plan: Option[JoinPlan]): Double = {
    plan match {
      case Some(joinPlan) =>
        // We use the negative cost as fitness.
        - joinPlan.planCost.card.toDouble * conf.joinReorderCardWeight -
            joinPlan.planCost.size.toDouble * (1 - conf.joinReorderCardWeight)
      case _ =>
        - Double.MaxValue
    }
  }
}

/**
 * A live space which has multi individuals(represented by [[Chromosome]]).
 * The population can evolve, generation by generation. The child individual
 * is generated by the [[Crossover]] procedure from its parents.
 */
object Population {
  def apply(
      conf: SQLConf,
      itemsMap: Map[Int, JoinPlan],
      conditions: Set[Expression],
      topOutputSet: AttributeSet) : Population = {

    val chromos: Seq[Chromosome] = Seq.fill(determinePopSize(conf, itemsMap.size)) {
      Chromosome(conf, shuffle(itemsMap), conditions, topOutputSet)
    }

    Population(conf, chromos)
  }

  private def determinePopSize(conf: SQLConf, numRelations: Int): Int = {
    val relaxFactor = conf.joinReorderGARelaxFactor
    // The default population size:
    // # of relations | pop size (RF=3) | pop size (RF=3.5)| pop size  (RF=4)
    //  < 13          |   DP based      |   DP based       |   DP based
    //    13          |   20            |   16 (13<16)     |   16
    //    14          |   25            |   16             |   16
    //    15          |   32            |   19             |   16
    //    16          |   40            |   23             |   16
    //    17          |   50            |   28             |   19
    //    18          |   64            |   35             |   22
    //    19          |   80            |   43             |   26
    //    20          |   101           |   52             |   32
    //    21          |   128           |   64             |   38
    //    22          |   128           |   78             |   45
    //    23          |   128           |   95             |   53
    //    24          |   128           |   115            |   64
    //    25          |   128           |   128            |   76
    //    26          |   128           |   128            |   90
    //    27          |   128           |   128            |   90
    //    28          |   128           |   128            |   128
    //  > 28          |   128           |   128            |   128
    val size = math.pow(2.0, numRelations / relaxFactor)
    val max = conf.joinReorderGAMaxPoPSize
    val min = conf.joinReorderGAMinPoPSize

    math.ceil(math.max(math.min(max, size), min)).toInt
  }

  private def shuffle(itemsMap: Map[Int, JoinPlan]) : Seq[JoinPlan] = {
    util.Random.shuffle(itemsMap.values).toSeq
  }
}

case class Population(conf: SQLConf, chromos: Seq[Chromosome]) extends Logging {

  def evolve: Population = {
    // Sort chromos in the population first.
    var evolvedChromos = chromos.sortWith((left, right) => left.fitness > right.fitness)
    // Begin iteration.
    val generations = chromos.size
    for (i <- 1 to generations) {
      val father = JoinReorderGAUtils.select(conf, evolvedChromos, None)
      val mother = JoinReorderGAUtils.select(conf, evolvedChromos, Some(father))
      val kid = EdgeRecombination.newChromo(father, mother)
      evolvedChromos = putToPop(kid, evolvedChromos)
      logDebug(s"Iteration $i, fitness for kid: ${kid.fitness}," +
          s" and Fitness for plans: ${ evolvedChromos.map(c => c.fitness)}")
    }
    Population(conf, evolvedChromos)
  }

  private def putToPop(kid: Chromosome, chromos: Seq[Chromosome]): Seq[Chromosome] = {
    val (l, r) = chromos.span(_.fitness > kid.fitness)
    ((l :+ kid) ++ r).init
  }
}

object JoinReorderUtils extends PredicateHelper {

  /**
   * Builds a new JoinPlan if the following conditions hold:
   * - the sets of items contained in left and right sides do not overlap.
   * - there exists at least one join condition involving references from both sides.
   * - if star-join filter is enabled, allow the following combinations:
   * 1) (oneJoinPlan U otherJoinPlan) is a subset of star-join
   * 2) star-join is a subset of (oneJoinPlan U otherJoinPlan)
   * 3) (oneJoinPlan U otherJoinPlan) is a subset of non star-join
   *
   * @param oneJoinPlan   One side JoinPlan for building a new JoinPlan.
   * @param otherJoinPlan The other side JoinPlan for building a new join node.
   * @param conf          SQLConf for statistics computation.
   * @param conditions    The overall set of join conditions.
   * @param topOutput     The output attributes of the final plan.
   * @param filters       Join graph info to be used as filters by the search algorithm.
   * @return Builds and returns a new JoinPlan if both conditions hold. Otherwise, returns None.
   */
  private[optimizer] def buildJoin(
      oneJoinPlan: JoinPlan,
      otherJoinPlan: JoinPlan,
      conf: SQLConf,
      conditions: Set[Expression],
      topOutput: AttributeSet,
      filters: Option[JoinGraphInfo]): Option[JoinPlan] = {

    if (oneJoinPlan.itemIds.intersect(otherJoinPlan.itemIds).nonEmpty) {
      // Should not join two overlapping item sets.
      return None
    }

    if (filters.isDefined) {
      // Apply star-join filter, which ensures that tables in a star schema relationship
      // are planned together. The star-filter will eliminate joins among star and non-star
      // tables until the star joins are built. The following combinations are allowed:
      // 1. (oneJoinPlan U otherJoinPlan) is a subset of star-join
      // 2. star-join is a subset of (oneJoinPlan U otherJoinPlan)
      // 3. (oneJoinPlan U otherJoinPlan) is a subset of non star-join
      val isValidJoinCombination =
      JoinReorderDPFilters.starJoinFilter(oneJoinPlan.itemIds, otherJoinPlan.itemIds,
        filters.get)
      if (!isValidJoinCombination) return None
    }

    val onePlan = oneJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    val joinConds = conditions
        .filterNot(l => canEvaluate(l, onePlan))
        .filterNot(r => canEvaluate(r, otherPlan))
        .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
    if (joinConds.isEmpty) {
      // Cartesian product is very expensive, so we exclude them from candidate plans.
      // This also significantly reduces the search space.
      return None
    }

    // Put the deeper side on the left, tend to build a left-deep tree.
    val (left, right) = if (oneJoinPlan.itemIds.size >= otherJoinPlan.itemIds.size) {
      (onePlan, otherPlan)
    } else {
      (otherPlan, onePlan)
    }
    val newJoin = Join(left, right, Inner, joinConds.reduceOption(And), JoinHint.NONE)
    val collectedJoinConds = joinConds ++ oneJoinPlan.joinConds ++ otherJoinPlan.joinConds
    val remainingConds = conditions -- collectedJoinConds
    val neededAttr = AttributeSet(remainingConds.flatMap(_.references)) ++ topOutput
    val neededFromNewJoin = newJoin.output.filter(neededAttr.contains)
    val newPlan =
      if ((newJoin.outputSet -- neededFromNewJoin).nonEmpty) {
        Project(neededFromNewJoin, newJoin)
      } else {
        newJoin
      }

    val itemIds = oneJoinPlan.itemIds.union(otherJoinPlan.itemIds)
    // Now the root node of onePlan/otherPlan becomes an intermediate join (if it's a non-leaf
    // item), so the cost of the new join should also include its own cost.
    val newPlanCost = oneJoinPlan.planCost + oneJoinPlan.rootCost(conf) +
        otherJoinPlan.planCost + otherJoinPlan.rootCost(conf)
    Some(JoinPlan(itemIds, newPlan, collectedJoinConds, newPlanCost))
  }
}

object JoinReorderGAUtils extends PredicateHelper {
  /**
   * Select a [[Chromosome]] randomly from the given chromosomes set. Chromosomes those in
   * excludes will not be selected.
   * The selection is biased, i.e., some chromosomes will be preferred compared to others.
   *
   * @param conf SQLConf for parameters that control the selection
   * @param chromos candidates to be selected.
   * @param exclude chromosome that will be excluded in the selection.
   * @return the selected chromosome.
   */
  def select(conf: SQLConf, chromos: Seq[Chromosome], exclude: Option[Chromosome]): Chromosome = {
    val exIndex = exclude.map { chromos.indexOf }.getOrElse(-1)
    var index = -1
    do {
      index = rand(chromos.size, conf.joinReorderGASelectionBias)
    } while (index == exIndex)
    chromos(index)
  }

  /**
   * A biased rand integer generator, which gives a random number in [0, cap)
   * and the number at small end will be chosen with higher probability.
   * The probability distribution is controlled by 'bias': with bigger bias,
   * the smaller number will be generated with higher probability. With bias be 0,
   * it will fallback to a equal-probability random integer generator on [0, cap)
   *
   * @param cap the upper bound of the range.
   * @param bias that controls the probability distribution.
   * @return the generated random integer.
   */
  private def rand(cap: Int, bias: Double) : Int = {
    assert(cap > 0)
    var index: Double = 0
    do {
      index = cap * (bias - math.sqrt((bias * bias) -
          4.0 * (bias - 1.0) * util.Random.nextDouble)) / 2.0 / (bias - 1.0)
    } while (index < 0.0 || index >= cap)

    index.toInt
  }
}
