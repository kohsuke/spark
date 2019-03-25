package org.apache.spark.graph.cypher

import org.apache.spark.graph.api.{CypherResult, NodeFrame, PropertyGraph, RelationshipFrame}
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table.RecordHeader

case class SparkCypherResult(relationalTable: RelationalCypherRecords[DataFrameTable], schema: Schema, session: SparkCypherSession) extends CypherResult {

  override val df: DataFrame = relationalTable.table.df

  private val header: RecordHeader = relationalTable.header

  override def graph: PropertyGraph = {
    val entityVars = relationalTable.header.entityVars
    val nodeVarNames = entityVars.collect { case v if v.cypherType.subTypeOf(CTNode) => v.name }
    val relVarNames = entityVars.collect { case v if v.cypherType.subTypeOf(CTRelationship) => v.name }

    val nodeFrames = nodeVarNames.flatMap(this.nodeFrames).toSeq
    val relFrames = relVarNames.flatMap(this.relationshipFrames).toSeq

    session.createGraph(nodeFrames, relFrames)
  }

  // TODO: Error handling
  override def nodeFrames(varName: String): Seq[NodeFrame] = {
    val nodeVar: NodeVar = find(NodeVar(varName)(CTNode))

    val idColumn = header.column(nodeVar)
    val possibleLabels = header.labelsFor(nodeVar).map(_.label.name)
    val labelCombinations = schema.combinationsFor(possibleLabels).toSeq

    val labelToColumns = header.labelsFor(nodeVar).map(expr => expr.label.name -> header.column(expr)).toMap
    val propertyToColumns = header.propertiesFor(nodeVar).map(expr => expr.key.name -> header.column(expr)).toMap

    val allLabelColumns = labelToColumns.values.toSet

    labelCombinations.map { labels =>
      val trueLabels = labels.map(labelToColumns)
      val falseLabels = allLabelColumns -- trueLabels

      val propertyKeys = schema.nodePropertyKeys(labels).keySet
      val properties = propertyToColumns.filter { case (propertyKey, _) => propertyKeys.contains(propertyKey) }
      val propertyColumns = properties.values.toSeq

      val selectColumns = (idColumn +: propertyColumns).map(escape)
      val labelCombinationDf = df
        .filter(trueLabels.map(df.col).map(_ === true).reduce(_ && _))
        .filter(falseLabels.map(df.col).map(_ === false).foldLeft(functions.lit(true))(_ && _))
        .select(selectColumns.head, selectColumns.tail: _*)
        .dropDuplicates(idColumn)

      NodeFrame(labelCombinationDf, idColumn, labels, properties)
    }
  }

  // TODO: Error handling
  override def relationshipFrames(varName: String): Seq[RelationshipFrame] = {
    val relVar: RelationshipVar = find(RelationshipVar(varName)(CTRelationship))

    val idColumn = header.column(relVar)
    val relTypes = header.typesFor(relVar).map(_.relType.name).toSeq
    val sourceIdColumn = header.column(header.startNodeFor(relVar))
    val targetIdColumn = header.column(header.endNodeFor(relVar))

    val relTypeToColumns = header.typesFor(relVar).map(expr => expr.relType.name -> header.column(expr)).toMap
    val propertyToColumns = header.propertiesFor(relVar).map(expr => expr.key.name -> header.column(expr)).toMap

    relTypes.map { relType =>
      val trueRelType = relTypeToColumns(relType)
      val propertyKeys = schema.relationshipPropertyKeys(relType).keySet
      val properties = propertyToColumns.filter { case (propertyKey, _) => propertyKeys.contains(propertyKey) }
      val propertyColumns = properties.values.toSeq

      val selectColumns = (idColumn +: sourceIdColumn +: targetIdColumn +: propertyColumns).map(escape)
      val relTypeDf = df
        .filter(df.col(trueRelType) === true)
        .select(selectColumns.head, selectColumns.tail: _*)
        .dropDuplicates(idColumn)

      RelationshipFrame(relTypeDf, idColumn, sourceIdColumn, targetIdColumn, relType, properties)
    }
  }

  private def escape(columnName: String): String = s"`$columnName`"

  // TODO use header.entityVars ?
  private def find[T <: ReturnItem](lookup: Var): T = relationalTable.header.idExpressions(lookup)
    .collectFirst { case expr if expr.withoutType == lookup.withoutType => expr }
    .getOrElse(throw IllegalArgumentException(
      expected = s"One of ${relationalTable.header.vars.map(_.withoutType).toList}",
      actual = lookup.name
    )).asInstanceOf[T]
}
