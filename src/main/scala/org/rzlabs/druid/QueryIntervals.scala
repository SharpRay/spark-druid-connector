package org.rzlabs.druid

import org.joda.time.{DateTime, Interval}
import org.rzlabs.druid.metadata.DruidRelationInfo

case class QueryIntervals(druidRelationInfo: DruidRelationInfo,
                          intervals: List[Interval] = Nil) {

  // The whole interval of Druid data source indexed data.
  val indexIntervals = druidRelationInfo.druidDataSource.intervals

  private def indexInterval(dt: DateTime): Option[Interval] = {
    indexIntervals.find(_.contains(dt))
  }

  def get: List[Interval] = if (intervals.isEmpty) indexIntervals else intervals

  /**
   * - if this is the first queryInterval, add it.
   * - if the new Interval overlaps with the current QueryInterval set interval to the overlap.
   * - otherwise, the interval is an empty Interval.
   * @param in
   * @return
   */
  private def add(in: Interval): QueryIntervals = {
    if (intervals.isEmpty) {
      // The first query interval. e.g., time < '2017-01-01T00:00:00'
      QueryIntervals(druidRelationInfo, List(in))
    } else {
      // new interval overlaps old.
      val oldIn = intervals.head
      if (oldIn.overlaps(in)) {
        // two interval overlaps.
        // e.g., time > '2017-01-01T00:00:00' and time < '2018-01-01T00:00:00'
        QueryIntervals(druidRelationInfo, List(oldIn.overlap(in)))
      } else {
        // e.g., time > '2017-01-01T00:00:00' and time < '2016-01-01T00:00:00'
        val idxIn = indexIntervals.head
        QueryIntervals(druidRelationInfo, List(idxIn.withEnd(idxIn.getStart)))
      }
    }
  }

  private def outsideIndexRange(dt: DateTime, ict: IntervalConditionType.Value): Option[QueryIntervals] = {
    if (indexIntervals.size == 1) { // This seems always be.
      ict match {
        case IntervalConditionType.LT | IntervalConditionType.LTE
          if indexIntervals(0).isBefore(dt) =>
          Some(add(indexIntervals(0)))
        case IntervalConditionType.GT | IntervalConditionType.GTE
          if indexIntervals(0).isAfter(dt) =>
          Some(add(indexIntervals(0)))
        case _ => None
      }
    } else None
  }

  def ltCond(dt: DateTime): Option[QueryIntervals] = {
    indexInterval(dt).map { in =>
      val newIn = in.withEnd(dt)
      add(newIn)
    } orElse {
      outsideIndexRange(dt, IntervalConditionType.LT)
    }
  }

  def lteCond(dt: DateTime): Option[QueryIntervals] = {
    indexInterval(dt).map { in =>
      val newIn = in.withEnd(dt.plusMillis(1)) // This because interval's end is excluded.
      add(newIn)
    } orElse {
      outsideIndexRange(dt, IntervalConditionType.LTE)
    }
  }

  def gtCond(dt: DateTime): Option[QueryIntervals] = {
    indexInterval(dt).map { in =>
      val newIn = in.withStart(dt.plusMillis(1)) // This because interval's end is included.
      add(newIn)
    } orElse {
      outsideIndexRange(dt, IntervalConditionType.GT)
    }
  }

  def gteCond(dt: DateTime): Option[QueryIntervals] = {
    indexInterval(dt).map { in =>
      val newIn = in.withStart(dt)
      add(newIn)
    } orElse {
      outsideIndexRange(dt, IntervalConditionType.GTE)
    }
  }

}
