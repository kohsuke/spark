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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  /**
   * Wrapper around an Expression that provides semantic equality.
   */
  case class Expr(e: Expression) {
    override def equals(o: Any): Boolean = o match {
      case other: Expr => e.semanticEquals(other.e)
      case _ => false
    }

    override def hashCode: Int = e.semanticHash()
  }

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[Expr, mutable.ArrayBuffer[Expression]]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (expr.deterministic) {
      val e: Expr = Expr(expr)
      val f = equivalenceMap.get(e)
      if (f.isDefined) {
        f.get += expr
        true
      } else {
        equivalenceMap.put(e, mutable.ArrayBuffer(expr))
        false
      }
    } else {
      false
    }
  }

  /**
   * Adds only expressions which are common in each of given expressions, in a recursive way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`,
   * the common expression `(c + 1)` will be added into `equivalenceMap`.
   */
  def addCommonExprs(exprs: Seq[Expression], addFunc: Expression => Boolean = addExpr): Unit = {
    var exprSetForAll = ExpressionSet()

    addExprTree(exprs.head, (expr: Expression) => {
      if (exprSetForAll.contains(expr)) {
        true
      } else {
        exprSetForAll += expr
        false
      }
    })

    exprs.tail.foreach { expr =>
      var exprSet = ExpressionSet()
      addExprTree(expr, (expr: Expression) => {
        if (exprSet.contains(expr)) {
          true
        } else {
          exprSet += expr
          false
        }
      })
      exprSetForAll = exprSetForAll.intersect(exprSet)
    }

    exprSetForAll.foreach(addFunc)
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      addFunc: Expression => Boolean = addExpr): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (expr.isInstanceOf[PlanExpression[_]] && TaskContext.get != null)

    // There are some special expressions that we should not recurse into all of its children.
    //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
    //   2. If: common subexpressions will always be evaluated at the beginning, but the true and
    //          false expressions in `If` may not get accessed, according to the predicate
    //          expression. We should only recurse into the predicate expression.
    //   3. CaseWhen: like `If`, the children of `CaseWhen` only get accessed in a certain
    //                condition. We should only recurse into the first condition expression as it
    //                will always get accessed.
    //   4. Coalesce: it's also a conditional expression, we should only recurse into the first
    //                children, because others may not get accessed.
    def childrenToRecurse: Seq[Expression] = expr match {
      case _: CodegenFallback => Nil
      case i: If => i.predicate :: Nil
      case c: CaseWhen => c.children.head :: Nil
      case c: Coalesce => c.children.head :: Nil
      case other => other.children
    }

    // For some special expressions we cannot just recurse into all of its children, but we can
    // recursively add the common expressions shared between all of its children.
    def commonChildrenToRecurse: Seq[Seq[Expression]] = expr match {
      case i: If => Seq(Seq(i.trueValue, i.falseValue))
      case c: CaseWhen =>
        val conditions = c.branches.tail.map(_._1)
        val values = c.branches.map(_._2) ++ c.elseValue
        Seq(conditions, values)
      case c: Coalesce => Seq(c.children.tail)
      case _ => Nil
    }

    if (!skip && !addFunc(expr)) {
      childrenToRecurse.foreach(addExprTree(_, addFunc))
      commonChildrenToRecurse.foreach(addCommonExprs(_, addFunc))
    }
  }

  /**
   * Returns all of the expression trees that are equivalent to `e`. Returns
   * an empty collection if there are none.
   */
  def getEquivalentExprs(e: Expression): Seq[Expression] = {
    equivalenceMap.getOrElse(Expr(e), Seq.empty).toSeq
  }

  /**
   * Returns all the equivalent sets of expressions.
   */
  def getAllEquivalentExprs: Seq[Seq[Expression]] = {
    equivalenceMap.values.map(_.toSeq).toSeq
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb: mutable.StringBuilder = new StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.foreach { case (k, v) =>
      if (all || v.length > 1) {
        sb.append("  " + v.mkString(", ")).append("\n")
      }
    }
    sb.toString()
  }
}
