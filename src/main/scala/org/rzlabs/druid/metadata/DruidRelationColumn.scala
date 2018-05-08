package org.rzlabs.druid.metadata

import org.rzlabs.druid._


case class DruidRelationColumnInfo(column: String,
                                   hllMetric: Option[String] = None,
                                   sketchMetric: Option[String] = None,
                                   cardinalityEstimate: Option[Long] = None)

case class DruidRelationColumn(column: String,
                               druidColumn: Option[DruidColumn],
                               hllMetric: Option[DruidMetric] = None,
                               sketchMetric: Option[DruidMetric] = None,
                               cardinalityEstimate: Option[Long] = None) {

  private lazy val druidColumnToUse: DruidColumn = {
    Utils.filterSomes(
      Seq(druidColumn, hllMetric, sketchMetric).toList
    ).head.get
  }

  def hasDirectDruidColumn = druidColumn.isDefined

  def hasHllMetric = hllMetric.isDefined

  def hasSketchMetric = sketchMetric.isDefined

  // TODO: Not support spatial index yet.
  def hasSpatialIndex = false

  //def name = druidColumnToUse.name
  def name = column

  //def dataType = if (hasSpatialIndex) DruidDataType.Float else druidColumnToUse.dataType
  def dataType = {
    if (hasSpatialIndex) {
      DruidDataType.Float
    } else if (isNotIndexedDimension) {
      // Specify non-indexed dimension type as string
      DruidDataType.String
    } else {
      druidColumnToUse.dataType
    }
  }

  def size = druidColumnToUse.size

  val cardinality: Long = cardinalityEstimate.getOrElse(druidColumnToUse.cardinality)

  def isDimension(excludeTime: Boolean = false): Boolean = {
    // Approximate metric refered dimension that not indexed in Druid datasource.
    hasDirectDruidColumn && druidColumnToUse.isDimension(excludeTime)
  }

  def isNotIndexedDimension = !hasDirectDruidColumn

  def isTimeDimension: Boolean = {
    hasDirectDruidColumn && druidColumnToUse.isInstanceOf[DruidTimeDimension]
  }

  def isMetric: Boolean = {
    hasDirectDruidColumn && !isDimension()
  }

  def metric = druidColumnToUse.asInstanceOf[DruidMetric]
}
