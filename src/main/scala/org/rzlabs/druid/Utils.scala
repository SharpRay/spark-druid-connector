package org.rzlabs.druid

import org.apache.spark.sql.MyLogging
import org.joda.time.Interval
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.rzlabs.druid.metadata.DruidRelationColumnInfo

object Utils extends MyLogging {

  implicit val jsonFormat = Serialization.formats(
    ShortTypeHints(
      List(
        classOf[DruidRelationColumnInfo],
        classOf[DurationGranularity],
        classOf[PeriodGranularity]
      )
    )
  ) + new DruidQueryGranularitySerializer

  def intervalsMillis(intervals: List[Interval]): Long = {
    intervals.foldLeft[Long](0L) {
      case (t, in) => t + (in.getEndMillis - in.getStartMillis)
    }
  }
}
