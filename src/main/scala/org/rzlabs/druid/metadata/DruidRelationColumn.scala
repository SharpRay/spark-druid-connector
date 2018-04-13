package org.rzlabs.druid.metadata

import org.rzlabs.druid.{DruidColumn, DruidMetric}


case class DruidRelationColumnInfo(column: String,
                                   hllMetric: Option[String] = None,
                                   sketchMetric: Option[String] = None,
                                   cardinalityEstimate: Option[Long] = None)

case class DruidRelationColumn(column: String,
                               druidColumn: DruidColumn,
                               hllMetric: Option[DruidMetric] = None,
                               sketchMetric: Option[DruidMetric] = None,
                               cardinalityEstimate: Option[Long] = None) {

}
