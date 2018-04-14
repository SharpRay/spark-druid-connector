package org.rzlabs.druid

import org.apache.spark.sql.types.{DataType, StringType}

/**
 * As defined in [[http://druid.io/docs/latest/querying/dimensionspecs.html]]
 */
sealed trait DimensionSpec {
  val `type`: String
  val dimension: String
  val outputName: String
//  val outputType: DataType

  def sparkDataType(druidDataSource: DruidDataSource): DataType = StringType
}

object DimensionOrderType extends Enumeration {
  val LEXICOGRAPHIC = Value("lexicographic")
  val ALPHANUMERIC = Value("alphanumeric")
  val STRLEN = Value("strlen")
  val NUMERIC = Value("numeric")
}

case class OrderByColumnSpec(dimension: String,
                             direction: String,
                             dimensionOrder: DimensionOrderType.Value = DimensionOrderType.LEXICOGRAPHIC) {

  def this(dimension: String, asc: Boolean) = {
    this(dimension, if (asc) "ascending" else "descending")
  }

  def this(dimension: String) = {
    this(dimension, "ascending")
  }
}

/**
 * As defined in [[http://druid.io/docs/latest/querying/limitspec.html]]
 * The key in druid is `limitSpec`.
 *
 * @param `type`
 * @param limit
 * @param columns
 */
case class LimitSpec(`type`: String,
                     limit: Int,
                     columns: List[OrderByColumnSpec] = Nil) {

  def this(limit: Int, columns: List[OrderByColumnSpec]) = {
    this("default", limit, columns)
  }

  def this(limit: Int, columns: OrderByColumnSpec*) = {
    this("default", limit, columns.toList)
  }
}

/**
 * As defined in [[http://druid.io/docs/latest/querying/having.html]]
 * The key in druid is `having`.
 */
sealed trait HavingSpec {
  val `type`: String
}

//case class QueryHavingSpec

/**
 *
 * @param `type` is 'greaterThan', 'equalTo' or 'lessThan'
 * @param aggregation The aggregation field output name
 * @param value The comparative value
 */
case class NumericHavingSpec(`type`: String,
                             aggregation: String,
                             val value: Double) extends HavingSpec


/**
 *
 * @param `type` is 'dimSelector'
 * @param dimension The dimension field
 * @param value The comparative dimension value
 */
case class DimensionSelectorHavingSpec(`type`: String,
                                       dimension: String,
                                       value: String) extends HavingSpec

/**
 *
 * @param `type` is 'and', 'or' or 'not'
 * @param havingSpecs The [[HavingSpec]] list.
 */
case class LogicalExpressionHavingSpec(`type`: String,
                                       havingSpecs: List[HavingSpec]) extends HavingSpec

