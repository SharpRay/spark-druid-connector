package org.rzlabs.druid

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
import org.rzlabs.druid.metadata.{DruidRelationColumn, DruidRelationInfo}

import scala.collection.mutable.{Map => MMap}

case class DruidQueryBuilder(druidRelationInfo: DruidRelationInfo,
                             queryIntervals: QueryIntervals,
                             referencedDruidColumns: MMap[String, DruidRelationColumn] = MMap(),
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec: Option[HavingSpec] = None,
                             granularitySpec: GranularitySpec = AllGranularitySpec("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: Option[List[PostAggregationSpec]] = None,
                             projectionAliasMap: Map[String, String] = Map(),
                             outputAttributeMap: Map[String, (Expression, DataType, DataType, String)] = Map(),
                            // avg expressions to perform in the Project operator
                            // on top of Druid PhysicalScan.
                             avgExpressions: Map[Expression, (String, String)] = Map(),
                             aggregateOper: Option[Aggregate] = None,
                             curId: AtomicLong = new AtomicLong(-1),
                             origProjectList: Option[Seq[NamedExpression]] = None,
                             origFilter: Option[Expression] = None,
                             hasUnpushedProjections: Boolean = false,
                             hasUnpushedFilters: Boolean = false
                            ) {

  override def toString = {
    s"""
      queryIntervals:
        ${queryIntervals.intervals.mkString("\n")}
      dimensions:
        ${dimensions.mkString("\n")}
       aggregations:
        ${aggregations.mkString("\n")}
     """.stripMargin
  }

  def isNonTimeDimension(name: String) = {
    druidColumn(name).map(_.isDimension(true)).getOrElse(false)
  }

  def dimensionSpec(d: DimensionSpec) = {
    this.copy(dimensions = dimensions :+ d)

  }

  def aggregationSpec(a: AggregationSpec) = {
    this.copy(aggregations = aggregations :+ a)
  }

  def filterSpecification(f: FilterSpec) = (filterSpec, f) match {
    case (Some(fs), _) =>
      this.copy(filterSpec = Some(new LogicalExpressionFilterSpec("and", List(f, fs))))
    case (None, _) =>
      this.copy(filterSpec = Some(f))
  }

  /**
   * Get the [[DruidRelationColumn]] by column name.
   * The column name may be alias name, so we should
   * find the real column name in ''projectionAliasMap''.
   * @param name The key to find DruidRelationColumn.
   * @return The found DruidRelationColumn or None.
   */
  def druidColumn(name: String): Option[DruidRelationColumn] = {
    druidRelationInfo.druidColumns.get(projectionAliasMap.getOrElse(name, name)).map {
      druidColumn =>
        referencedDruidColumns(name) = druidColumn
        druidColumn
    }
  }

  def queryInterval(ic: IntervalCondition): Option[DruidQueryBuilder] = ic.`type` match {
    case IntervalConditionType.LT =>
      queryIntervals.ltCond(ic.dt).map(qi => this.copy(queryIntervals = qi))
    case IntervalConditionType.LTE =>
      queryIntervals.lteCond(ic.dt).map(qi => this.copy(queryIntervals = qi))
    case IntervalConditionType.GT =>
      queryIntervals.gtCond(ic.dt).map(qi => this.copy(queryIntervals = qi))
    case IntervalConditionType.GTE =>
      queryIntervals.gteCond(ic.dt).map(qi => this.copy(queryIntervals = qi))
  }

  /**
   * From "alias-1" to "alias-N".
   * @return
   */
  def nextAlias: String = s"alias${curId.getAndDecrement()}"

  def outputAttribute(name: String, e: Expression, originalDT: DataType,
                      druidDT: DataType, tfName: String = null) = {
    val tf = if (tfName == null) DruidValTransform.getTFName(druidDT) else tfName
    this.copy(outputAttributeMap = outputAttributeMap + (name -> (e, originalDT, druidDT, tf)))
  }

  def addAlias(alias: String, col: String) = {
    val colName = projectionAliasMap.getOrElse(col, col)
    this.copy(projectionAliasMap = projectionAliasMap + (alias -> colName))
  }

  def avgExpression(e: Expression, sumAlias: String, countAlias: String) = {
    this.copy(avgExpressions = avgExpressions + (e -> (sumAlias, countAlias)))
  }

}

object DruidQueryBuilder {
  def apply(druidRelationInfo: DruidRelationInfo) = {
    new DruidQueryBuilder(druidRelationInfo, new QueryIntervals(druidRelationInfo))
  }
}
