package org.rzlabs.druid.metadata


case class DruidRelationColumnInfo(
                                    column: String,
                                    hllMetric: Option[String] = None,
                                    sketchMetric: Option[String] = None,
                                    cardinalityEstimate: Option[Long] = None
                                    )
