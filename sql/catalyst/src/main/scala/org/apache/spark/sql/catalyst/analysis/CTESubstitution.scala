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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Except, LogicalPlan, RecursiveRelation, SubqueryAlias, Union, With}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LEGACY_CTE_PRECEDENCE_ENABLED

/**
 * Analyze WITH nodes and substitute child plan with CTE definitions.
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.getConf(LEGACY_CTE_PRECEDENCE_ENABLED)) {
      legacyTraverseAndSubstituteCTE(plan)
    } else {
      traverseAndSubstituteCTE(plan, false)
    }
  }

  private def legacyTraverseAndSubstituteCTE(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child, relations, allowRecursion) =>
        // substitute CTE expressions right-to-left to resolve references to previous CTEs:
        // with a as (select * from t), b as (select * from a) select * from b
        relations.foldRight(child) {
          case ((cteName, ctePlan), currentPlan) =>
            val recursionHandledPlan = handleRecursion(ctePlan, cteName, allowRecursion)
            substituteCTE(currentPlan, cteName, recursionHandledPlan)
        }
    }
  }

  /**
   * Traverse the plan and expression nodes as a tree and replace matching references to CTE
   * definitions.
   * - If the rule encounters a WITH node then it substitutes the child of the node with CTE
   *   definitions of the node right-to-left order as a definition can reference to a previous
   *   one.
   *   For example the following query is valid:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (SELECT * FROM t)
   *   SELECT * FROM t2
   * - If a CTE definition contains an inner WITH node then substitution of inner should take
   *   precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (
   *       WITH t AS (SELECT 2)
   *       SELECT * FROM t
   *     )
   *   SELECT * FROM t2
   * - If a CTE definition contains a subquery that contains an inner WITH node then substitution
   *   of inner should take precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1 AS c)
   *   SELECT max(c) FROM (
   *     WITH t AS (SELECT 2 AS c)
   *     SELECT * FROM t
   *   )
   * - If a CTE definition contains a subquery expression that contains an inner WITH node then
   *   substitution of inner should take precedence because it can shadow an outer CTE
   *   definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1)
   *   SELECT (
   *     WITH t AS (SELECT 2)
   *     SELECT * FROM t
   *   )
   * @param plan the plan to be traversed
   * @param inTraverse whether the current traverse is called from another traverse, only in this
   *                   case name collision can occur
   * @return the plan where CTE substitution is applied
   */
  private def traverseAndSubstituteCTE(plan: LogicalPlan, inTraverse: Boolean): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child: LogicalPlan, relations, allowRecursion) =>
        // child might contain an inner CTE that has priority so traverse and substitute inner CTEs
        // in child first
        val traversedChild: LogicalPlan = child transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }

        // Substitute CTE definitions from last to first as a CTE definition can reference a
        // previous one
        relations.foldRight(traversedChild) {
          case ((cteName, ctePlan), currentPlan) =>
            // A CTE definition might contain an inner CTE that has priority, so traverse and
            // substitute CTE defined in ctePlan.
            // A CTE definition might not be used at all or might be used multiple times. To avoid
            // computation if it is not used and to avoid multiple recomputation if it is used
            // multiple times we use a lazy construct with call-by-name parameter passing.
            lazy val substitutedCTEPlan = traverseAndSubstituteCTE(ctePlan, true)
            lazy val recursionHandledPlan =
              handleRecursion(substitutedCTEPlan, cteName, allowRecursion)
            substituteCTE(currentPlan, cteName, recursionHandledPlan)
        }

      // CTE name collision can occur only when inTraverse is true, it helps to avoid eager CTE
      // substitution in a subquery expression.
      case other if inTraverse =>
        other.transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }
    }
  }

  /**
   * If recursion is allowed recursion handling starts with inserting unresolved self-references
   * ([[UnresolvedRecursiveReference]]) to places where a reference to the CTE definition itself is
   * found.
   * If there is a self-reference then we need to check if structure of the query satisfies the SQL
   * recursion rules and insert the appropriate [[RecursiveRelation]] finally.
   */
  private def handleRecursion(
      ctePlan: => LogicalPlan,
      cteName: String,
      allowRecursion: Boolean) = {
    if (allowRecursion) {
      // check if there is any reference to the CTE and if there is then treat the CTE as recursive
      val (recursiveReferencesPlan, recursiveReferenceCount) =
        insertRecursiveReferences(ctePlan, cteName)
      if (recursiveReferenceCount > 0) {
        // if there is a reference then the CTE needs to follow one of these structures
        recursiveReferencesPlan match {
          case SubqueryAlias(_, u: Union) =>
            insertRecursiveRelation(cteName, Seq.empty, false, u)
          case SubqueryAlias(_, Distinct(u: Union)) =>
            insertRecursiveRelation(cteName, Seq.empty, true, u)
          case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, u: Union)) =>
            insertRecursiveRelation(cteName, columnNames, false, u)
          case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, Distinct(u: Union))) =>
            insertRecursiveRelation(cteName, columnNames, true, u)
          case _ =>
            throw new AnalysisException(s"Recursive query ${cteName} should contain UNION or " +
              s"UNION ALL statements only. This error can also be caused by ORDER BY or LIMIT " +
              s"keywords used on result of UNION or UNION ALL.")
        }
      } else {
        ctePlan
      }
    } else {
      ctePlan
    }
  }

  /**
   * If we encounter a relation that matches the recursive CTE then the relation is replaced to an
   * [[UnresolvedRecursiveReference]]. The replacement process also checks possible references in
   * subqueries and report them as errors.
   */
  private def insertRecursiveReferences(
      ctePlan: LogicalPlan,
      cteName: String): (LogicalPlan, Int) = {
    var recursiveReferenceCount = 0
    val resolver = ctePlan.conf.resolver
    val newPlan = ctePlan resolveOperators {
      case UnresolvedRelation(Seq(table)) if (ctePlan.conf.resolver(cteName, table)) =>
        recursiveReferenceCount += 1
        UnresolvedRecursiveReference(cteName, false)

      case other =>
        other.subqueries.foreach(checkAndTraverse(_, {
          case UnresolvedRelation(Seq(name)) if (resolver(cteName, name)) =>
            throw new AnalysisException(s"Recursive query ${cteName} should not contain " +
              "recursive references in its subquery.")
          case _ => true
        }))
        other
    }

    (newPlan, recursiveReferenceCount)
  }

  private def insertRecursiveRelation(
      cteName: String,
      columnNames: Seq[String],
      distinct: Boolean,
      union: Union) = {
    if (union.children.size != 2) {
      throw new AnalysisException(s"Recursive query ${cteName} should contain one anchor term " +
        s"and one recursive term connected with UNION or UNION ALL.")
    }

    val anchorTerm :: recursiveTerm :: Nil = union.children

    // The anchor term shouldn't contain a recursive reference that matches the name of the CTE,
    // except if it is nested to another RecursiveRelation
    checkAndTraverse(anchorTerm, {
      case UnresolvedRecursiveReference(name, _) if name == cteName =>
        throw new AnalysisException(s"Recursive query ${cteName} should not contain recursive " +
          "references in its anchor (first) term.")
      case RecursiveRelation(name, _, _) if name == cteName => false
      case _ => true
    })

    // The anchor term has a special role, its output column are aliased if required.
    val aliasedAnchorTerm = SubqueryAlias(cteName,
      if (columnNames.nonEmpty) {
        UnresolvedSubqueryColumnAliases(columnNames, anchorTerm)
      } else {
        anchorTerm
      }
    )

    // If UNION combinator is used between the terms we extend the anchor with a DISTINCT and the
    // recursive term with an EXCEPT clause and a reference to the so far cumulated result.
    if (distinct) {
      RecursiveRelation(cteName, Distinct(aliasedAnchorTerm),
        Except(recursiveTerm, UnresolvedRecursiveReference(cteName, true), false))
    } else {
      RecursiveRelation(cteName, aliasedAnchorTerm, recursiveTerm)
    }
  }

  /**
   * Taverses the plan including subqueries until the check function returns true.
   */
  private def checkAndTraverse(plan: LogicalPlan, check: LogicalPlan => Boolean): Unit = {
    if (check(plan)) {
      plan.children.foreach(checkAndTraverse(_, check))
      plan.subqueries.foreach(checkAndTraverse(_, check))
    }
  }

  private def substituteCTE(
      plan: LogicalPlan,
      cteName: String,
      ctePlan: => LogicalPlan): LogicalPlan =
    plan resolveOperatorsUp {
      case UnresolvedRelation(Seq(table)) if plan.conf.resolver(cteName, table) => ctePlan

      case other =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        other transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
        }
    }
}
