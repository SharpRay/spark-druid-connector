package org.rzlabs.druid

import org.joda.time.Interval
import org.rzlabs.druid.metadata.DruidRelationInfo

case class QueryIntervals(relationInfo: DruidRelationInfo,
                          intervals: List[Interval] = Nil) {


}
