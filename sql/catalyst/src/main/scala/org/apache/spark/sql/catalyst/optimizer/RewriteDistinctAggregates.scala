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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.IntegerType

/**
 * This rule rewrites an aggregate query with distinct aggregations into an expanded double
 * aggregation in which the regular aggregation expressions and every distinct clause is aggregated
 * in a separate group. The results are then combined in a second aggregate.
 *
 * First example: query without filter clauses (in scala):
 * {{{
 *   val data = Seq(
 *     (1, "a", "ca1", "cb1", 10),
 *     (2, "a", "ca1", "cb2", 5),
 *     (3, "b", "ca1", "cb1", 13))
 *     .toDF("id", "key", "cat1", "cat2", "value")
 *   data.createOrReplaceTempView("data")
 *
 *   val agg = data.groupBy($"key")
 *     .agg(
 *       countDistinct($"cat1").as("cat1_cnt"),
 *       countDistinct($"cat2").as("cat2_cnt"),
 *       sum($"value").as("total"))
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count(if (('gid = 1)) 'cat1 else null),
 *                 count(if (('gid = 2)) 'cat2 else null),
 *                 first(if (('gid = 0)) 'total else null) ignore nulls]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'cat1, 'cat2, 'gid]
 *      functions = [sum('value)]
 *      output = ['key, 'cat1, 'cat2, 'gid, 'total])
 *     Expand(
 *        projections = [('key, null, null, 0, cast('value as bigint)),
 *                       ('key, 'cat1, null, 1, null),
 *                       ('key, null, 'cat2, 2, null)]
 *        output = ['key, 'cat1, 'cat2, 'gid, 'value])
 *       LocalTableScan [...]
 * }}}
 *
 * Second example: aggregate function without distinct and with filter clauses (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) as cat1_cnt,
 *     COUNT(DISTINCT cat2) as cat2_cnt,
 *     SUM(value) FILTER (WHERE id > 1) AS total
 *  FROM
 *    data
 *  GROUP BY
 *    key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value) with FILTER('id > 1)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count(if (('gid = 1)) '_gen_attr_1 else null),
 *                 count(if (('gid = 2)) '_gen_attr_2 else null),
 *                 first(if (('gid = 0)) 'total else null) ignore nulls]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, '_gen_attr_1, '_gen_attr_2, 'gid]
 *      functions = [sum('_gen_attr_3)]
 *      output = ['key, '_gen_attr_1, '_gen_attr_2, 'gid, 'total])
 *     Expand(
 *        projections = [('key, null, null, 0, if ('id > 1) cast('value as bigint) else null, 'id),
 *                       ('key, 'cat1, null, 1, null, null),
 *                       ('key, null, 'cat2, 2, null, null)]
 *        output = ['key, '_gen_attr_1, '_gen_attr_2, 'gid, '_gen_attr_3, 'id])
 *       LocalTableScan [...]
 * }}}
 *
 * Third example: single distinct aggregate function with filter clauses and have
 * not other distinct aggregate function (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) FILTER (WHERE id > 1) as cat1_cnt,
 *     SUM(value) AS total
 *  FROM
 *    data
 *  GROUP BY
 *    key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1) with FILTER('id > 1),
 *                 sum('value)]
 *    output = ['key, 'cat1_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 *   Aggregate(
 *      key = ['key]
 *      functions = [count('_gen_attr_1),
  *                  sum('_gen_attr_2)]
 *      output = ['key, 'cat1_cnt, 'total])
 *     Project(
 *        projectionList = ['key, if ('id > 1) 'cat1 else null, cast('value as bigint)]
 *        output = ['key, '_gen_attr_1, '_gen_attr_2])
 *       LocalTableScan [...]
 * }}}
 *
 * Four example: single distinct aggregate function with filter clauses (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) FILTER (WHERE id > 1) as cat1_cnt,
 *     COUNT(DISTINCT cat2) as cat2_cnt,
 *     SUM(value) AS total
 *  FROM
 *    data
 *  GROUP BY
 *    key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1) with FILTER('id > 1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 *   Aggregate(
 *      key = ['key]
 *      functions = [count(if (('gid = 1)) '_gen_attr_1 else null),
 *                   count(if (('gid = 2)) '_gen_attr_2 else null),
 *                   first(if (('gid = 0)) 'total else null) ignore nulls]
 *      output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *     Aggregate(
 *        key = ['key, '_gen_attr_1, '_gen_attr_2, 'gid]
 *        functions = [sum('_gen_attr_3)]
 *        output = ['key, '_gen_attr_1, '_gen_attr_2, 'gid, 'total])
 *       Expand(
 *          projections = [('key, null, null, 0, cast('value as bigint)),
 *                         ('key, if ('id > 1) 'cat1 else null, null, 1, null),
 *                         ('key, null, 'cat2, 2, null)]
 *          output = ['key, '_gen_attr_1, '_gen_attr_2, 'gid, '_gen_attr_3])
 *         LocalTableScan [...]
 * }}}
 *
 * The rule consists of the two phases as follows:
 *
 * In the first phase, if the aggregate query exists filter clauses, project the output of
 * the child of the aggregate query:
 * 1. Project the data. There are three aggregation groups in this query:
 *    i. the non-distinct group;
 *    ii. the distinct 'cat1 group;
 *    iii. the distinct 'cat2 group with filter clause.
 *    Because there is at least one group with filter clause (e.g. the distinct 'cat2
 *    group with filter clause), then will project the data.
 * 2. Avoid projections that may output the same attributes. There are three aggregation groups
 *    in this query:
 *    i. the non-distinct 'cat1 group;
 *    ii. the distinct 'cat1 group;
 *    iii. the distinct 'cat1 group with filter clause.
 *    The attributes referenced by different aggregate expressions are likely to overlap,
 *    and if no additional processing is performed, data loss will occur. If we directly output
 *    the attributes of the aggregate expression, we may get three attributes 'cat1. To prevent
 *    this, we generate new attributes (e.g. '_gen_attr_1) and replace the original ones.
 *
 * Why we need the first phase? guaranteed to compute filter clauses in the first aggregate
 * locally.
 * Note: after generate new attributes, the aggregate may have at least two distinct groups,
 * so we need the second phase too.
 *
 * In the second phase, rewrite a query with two or more distinct groups:
 * 1. Expand the data. There are three aggregation groups in this query:
 *    i. the non-distinct group;
 *    ii. the distinct 'cat1 group;
 *    iii. the distinct 'cat2 group.
 *    An expand operator is inserted to expand the child data for each group. The expand will null
 *    out all unused columns for the given group; this must be done in order to ensure correctness
 *    later on. Groups can by identified by a group id (gid) column added by the expand operator.
 * 2. De-duplicate the distinct paths and aggregate the non-aggregate path. The group by clause of
 *    this aggregate consists of the original group by clause, all the requested distinct columns
 *    and the group id. Both de-duplication of distinct column and the aggregation of the
 *    non-distinct group take advantage of the fact that we group by the group id (gid) and that we
 *    have nulled out all non-relevant columns the given group.
 * 3. Aggregating the distinct groups and combining this with the results of the non-distinct
 *    aggregation. In this step we use the group id to filter the inputs for the aggregate
 *    functions. The result of the non-distinct group are 'aggregated' by using the first operator,
 *    it might be more elegant to use the native UDAF merge mechanism for this in the future.
 * 4. If the first phase inserted a project operator as the child of aggregate and the second phase
 *    already decided to insert an expand operator as the child of aggregate, the second phase will
 *    merge the project operator with expand operator.
 *
 * This rule duplicates the input data by two or more times (# distinct groups + an optional
 * non-distinct group). This will put quite a bit of memory pressure of the used aggregate and
 * exchange operators. Keeping the number of distinct groups as low as possible should be priority,
 * we could improve this in the current rule by applying more advanced expression canonicalization
 * techniques.
 */
object RewriteDistinctAggregates extends Rule[LogicalPlan] {

  private def mayNeedtoRewrite(a: Aggregate): Boolean = {
    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)
    // We need at least two distinct aggregates or the aggregate query exists filter clause for
    // this rule because aggregation strategy can handle a single distinct group without a filter.
    // This check can produce false-positives, e.g., SUM(DISTINCT a) & COUNT(DISTINCT a).
    distinctAggs.size > 1 || aggExpressions.exists(_.filter.isDefined)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case a: Aggregate if mayNeedtoRewrite(a) =>
      val (aggregate, projected) = projectFiltersInAggregates(a)
      rewriteDistinctAggregates(aggregate, projected)
  }

  private def projectFiltersInAggregates(a: Aggregate): (Aggregate, Boolean) = {
    val aggExpressions = collectAggregateExprs(a)
    if (aggExpressions.exists(_.filter.isDefined)) {
      // Constructs pairs between old and new expressions for aggregates.
      val aggExprs = aggExpressions.filter(e => e.children.exists(!_.foldable))
      val (projections, aggPairs) = aggExprs.map {
        case ae @ AggregateExpression(af, _, _, filter, _) =>
          // First, In order to reduce costs, it is better to handle the filter clause locally.
          // e.g. COUNT (DISTINCT a) FILTER (WHERE id > 1), evaluate expression
          // If(id > 1) 'a else null first, and use the result as output.
          // Second, If at least two DISTINCT aggregate expression which may references the
          // same attributes. We need to construct the generated attributes so as the output not
          // lost. e.g. SUM (DISTINCT a), COUNT (DISTINCT a) FILTER (WHERE id > 1) will output
          // attribute '_gen_attr-1 and attribute '_gen_attr-2 instead of two 'a.
          // Note: The illusionary mechanism may result in at least two distinct groups, so we
          // still need to call `rewriteDistinctAggregates`.
          val unfoldableChildren = af.children.filter(!_.foldable)
          // Expand projection
          val projectionMap = unfoldableChildren.map {
            case e if filter.isDefined =>
              val ife = If(filter.get, e, nullify(e))
              e -> Alias(ife, s"_gen_attr_${NamedExpression.newExprId.id}")()
            // For convenience and unification, we always alias the column, even if
            // there is no filter.
            case e => e -> Alias(e, s"_gen_attr_${NamedExpression.newExprId.id}")()
          }
          val projection = projectionMap.map(_._2)
          val exprAttrs = projectionMap.map { kv =>
            (kv._1, kv._2.toAttribute)
          }
          val exprAttrLookup = exprAttrs.toMap
          val newChildren = af.children.map(c => exprAttrLookup.getOrElse(c, c))
          val raf = af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
          val aggExpr = ae.copy(aggregateFunction = raf, filter = None)
          (projection, (ae, aggExpr))
      }.unzip
      // Construct the aggregate input projection.
      val namedGroupingExpressions = a.groupingExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, other.toString)()
      }
      val rewriteAggProjection = namedGroupingExpressions ++ projections.flatten
      // Construct the project operator.
      val project = Project(rewriteAggProjection, a.child)
      val groupByAttrs = namedGroupingExpressions.map(_.toAttribute)
      val rewriteAggExprLookup = aggPairs.toMap
      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case ae: AggregateExpression => rewriteAggExprLookup.getOrElse(ae, ae)
        }.asInstanceOf[NamedExpression]
      }
      (Aggregate(groupByAttrs, patchedAggExpressions, project), true)
    } else {
      (a, false)
    }
  }

  private def rewriteDistinctAggregates(a: Aggregate, projected: Boolean): Aggregate = {
    val aggExpressions = collectAggregateExprs(a)

    // Extract distinct aggregate expressions.
    val distinctAggGroups = aggExpressions.filter(_.isDistinct).groupBy { e =>
        val unfoldableChildren = e.aggregateFunction.children.filter(!_.foldable).toSet
        if (unfoldableChildren.nonEmpty) {
          // Only expand the unfoldable children
          unfoldableChildren
        } else {
          // If aggregateFunction's children are all foldable
          // we must expand at least one of the children (here we take the first child),
          // or If we don't, we will get the wrong result, for example:
          // count(distinct 1) will be explained to count(1) after the rewrite function.
          // Generally, the distinct aggregateFunction should not run
          // foldable TypeCheck for the first child.
          e.aggregateFunction.children.take(1).toSet
        }
    }

    // Aggregation strategy can handle queries with a single distinct group.
    if (distinctAggGroups.size > 1) {
      // Create the attributes for the grouping id and the group by clause.
      val gid = AttributeReference("gid", IntegerType, nullable = false)()
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
      }
      val groupByAttrs = groupByMap.map(_._2)

      // Functions used to modify aggregate functions and their inputs.
      def evalWithinGroup(id: Literal, e: Expression) = If(EqualTo(gid, id), e, nullify(e))
      def patchAggregateFunctionChildren(
          af: AggregateFunction)(
          attrs: Expression => Option[Expression]): AggregateFunction = {
        val newChildren = af.children.map(c => attrs(c).getOrElse(c))
        af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
      }

      // Setup unique distinct aggregate children.
      val distinctAggChildren = distinctAggGroups.keySet.flatten.toSeq.distinct
      val distinctAggChildAttrMap = distinctAggChildren.map(expressionAttributePair)
      val distinctAggChildAttrs = distinctAggChildAttrMap.map(_._2)

      // Setup expand & aggregate operators for distinct aggregate expressions.
      val distinctAggChildAttrLookup = distinctAggChildAttrMap.toMap
      val distinctAggOperatorMap = distinctAggGroups.toSeq.zipWithIndex.map {
        case ((group, expressions), i) =>
          val id = Literal(i + 1)

          // Expand projection
          val projection = distinctAggChildren.map {
            case e if group.contains(e) => e
            case e => nullify(e)
          } :+ id

          // Final aggregate
          val operators = expressions.map { e =>
            val af = e.aggregateFunction
            val naf = patchAggregateFunctionChildren(af) { x =>
              distinctAggChildAttrLookup.get(x).map(evalWithinGroup(id, _))
            }
            (e, e.copy(aggregateFunction = naf, isDistinct = false))
          }

          (projection, operators)
      }

      // Setup expand for the 'regular' aggregate expressions.
      // only expand unfoldable children
      val regularAggExprs = aggExpressions
        .filter(e => !e.isDistinct && e.children.exists(!_.foldable))
      val regularAggChildren = regularAggExprs
        .flatMap(_.aggregateFunction.children.filter(!_.foldable))
        .distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair)

      // Setup aggregates for 'regular' aggregate expressions.
      val regularGroupId = Literal(0)
      val regularAggChildAttrLookup = regularAggChildAttrMap.toMap
      val regularAggOperatorMap = regularAggExprs.map { e =>
        // Perform the actual aggregation in the initial aggregate.
        val af = patchAggregateFunctionChildren(e.aggregateFunction)(regularAggChildAttrLookup.get)
        val operator = Alias(e.copy(aggregateFunction = af), e.sql)()

        // Select the result of the first aggregate in the last aggregate.
        val result = AggregateExpression(
          aggregate.First(evalWithinGroup(regularGroupId, operator.toAttribute), Literal(true)),
          mode = Complete,
          isDistinct = false)

        // Some aggregate functions (COUNT) have the special property that they can return a
        // non-null result without any input. We need to make sure we return a result in this case.
        val resultWithDefault = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(result, lit))
          case None => result
        }

        // Return a Tuple3 containing:
        // i. The original aggregate expression (used for look ups).
        // ii. The actual aggregation operator (used in the first aggregate).
        // iii. The operator that selects and returns the result (used in the second aggregate).
        (e, operator, resultWithDefault)
      }

      // Construct the regular aggregate input projection only if we need one.
      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          distinctAggChildren.map(nullify) ++
          Seq(regularGroupId) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }

      // Construct the distinct aggregate input projections.
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjections = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      val (projections, expandChild) = if (projected) {
        // If `projectFiltersInAggregates` inserts Project as child of Aggregate and
        // `rewriteDistinctAggregates` will insert Expand here, merge Project with the Expand.
        val projectAttributeExpressionMap = a.child.asInstanceOf[Project].projectList.map {
          case ne: NamedExpression => ne.name -> ne
        }.toMap
        val projections = (regularAggProjection ++ distinctAggProjections).map {
          case projection: Seq[Expression] => projection.map {
            case ne: NamedExpression => projectAttributeExpressionMap.getOrElse(ne.name, ne)
            case other => other
          }
        }
        (projections, a.child.asInstanceOf[Project].child)
      } else {
        (regularAggProjection ++ distinctAggProjections, a.child)
      }
      // Construct the expand operator.
      val expand = Expand(
        projections,
        groupByAttrs ++ distinctAggChildAttrs ++ Seq(gid) ++ regularAggChildAttrMap.map(_._2),
        expandChild)

      // Construct the first aggregate operator. This de-duplicates all the children of
      // distinct operators, and applies the regular aggregate operators.
      val firstAggregateGroupBy = groupByAttrs ++ distinctAggChildAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ regularAggOperatorMap.map(_._2),
        expand)

      // Construct the second aggregate
      val transformations: Map[Expression, Expression] =
        (distinctAggOperatorMap.flatMap(_._2) ++
          regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            // The same GROUP BY clauses can have different forms (different names for instance) in
            // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
            // tricky. So we do a linear search for a semantically equal group by expression.
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    } else {
      a
    }
  }

  private def collectAggregateExprs(a: Aggregate): Seq[AggregateExpression] = {
    a.aggregateExpressions.flatMap { e =>
      e.collect {
        case ae: AggregateExpression => ae
      }
    }
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  private def expressionAttributePair(e: Expression) =
    // We are creating a new reference here instead of reusing the attribute in case of a
    // NamedExpression. This is done to prevent collisions between distinct and regular aggregate
    // children, in this case attribute reuse causes the input of the regular aggregate to bound to
    // the (nulled out) input of the distinct aggregate.
    e -> AttributeReference(e.sql, e.dataType, nullable = true)()
}
